from __future__ import absolute_import

import operator
import os
import re
import time

import rrdtool

from os.path import isdir, isfile, join, basename
from structlog import get_logger

from ..node import BranchNode
from ..node import LeafNode
from ..intervals import Interval
from ..intervals import IntervalSet
from . import match_entries

logger = get_logger()

search_ds_name = re.compile('^ds\[(.*?)\]\.index$').search
search_rra_index = re.compile('^rra\[(.*?)\].pdp_per_row$').search

from . import fs_to_metric, get_real_metric_path, match_entries, find_escaped_pattern_fields, convert_fs_path


class RRDToolFinder(object):
    DATASOURCE_DELIMITER = '::RRD_DATASOURCE::'

    def __init__(self, config):
        directories = config['rrdtool']['directories']
        self.directories = directories
        self.flush = config['rrdtool']['flushrrdcached']
        self.cf = config['rrdtool']['cf']

    def find_nodes(self, query):
        clean_pattern = query.pattern.replace('\\', '')
        pattern_parts = clean_pattern.split('.')

        for root_dir in self.directories:
            for absolute_path in self._find_paths(root_dir, pattern_parts):
                if basename(absolute_path).startswith('.'):
                    continue

                if self.DATASOURCE_DELIMITER in basename(absolute_path):
                    (absolute_path, datasource_pattern) = absolute_path.rsplit(self.DATASOURCE_DELIMITER, 1)
                else:
                    datasource_pattern = None

                relative_path = absolute_path[ len(root_dir): ].lstrip('/')
                metric_path = fs_to_metric(relative_path)
                real_metric_path = get_real_metric_path(absolute_path, metric_path)

                metric_path_parts = metric_path.split('.')
                for field_index in find_escaped_pattern_fields(query.pattern):
                    metric_path_parts[field_index] = pattern_parts[field_index].replace('\\', '')
                metric_path = '.'.join(metric_path_parts)

                # Now we construct and yield an appropriate Node object
                if isdir(absolute_path):
                    yield BranchNode(metric_path)

                elif absolute_path.endswith('.rrd'):
                    if datasource_pattern is None:
                        yield BranchNode(metric_path)

                    else:
                        for datasource_name in RRDReader.get_datasources(absolute_path):
                            if match_entries([datasource_name], datasource_pattern):
                                reader = RRDReader(absolute_path, datasource_name, self.cf, self.flush)
                                yield LeafNode(metric_path + "." + datasource_name, reader)

    def _find_paths(self, current_dir, patterns):
        """Recursively generates absolute paths whose components underneath current_dir
        match the corresponding pattern in patterns"""
        pattern = patterns[0]
        patterns = patterns[1:]

        has_wildcard = pattern.find('{') > -1 or pattern.find('[') > -1 or pattern.find('*') > -1 or pattern.find('?') > -1
        using_globstar = pattern == "**"

        if has_wildcard: # this avoids os.listdir() for performance
            try:
                entries = os.listdir(current_dir)
            except OSError as e:
                logger.error(e)
                entries = []
        else:
            entries = [ pattern ]

        if using_globstar:
            matching_subdirs = map(operator.itemgetter(0), os.walk(current_dir))
        else:
            subdirs = [entry for entry in entries if isdir(join(current_dir, entry))]
            matching_subdirs = match_entries(subdirs, pattern)

        # if this is a terminal globstar, add a pattern for all files in subdirs
        if using_globstar and not patterns:
            patterns = ["*"]

        if len(patterns) == 1:
            if not has_wildcard:
                entries = [ pattern + ".rrd" ]
            files = [entry for entry in entries if isfile(join(current_dir, entry))]
            rrd_files = match_entries(files, pattern + ".rrd")

            if rrd_files: #let's assume it does
                datasource_pattern = patterns[0]

            for rrd_file in rrd_files:
                absolute_path = join(current_dir, rrd_file)
                yield absolute_path + self.DATASOURCE_DELIMITER + datasource_pattern

        if patterns: #we've still got more directories to traverse
            for subdir in matching_subdirs:

                absolute_path = join(current_dir, subdir)
                for match in self._find_paths(absolute_path, patterns):
                    yield match

        else: #we've got the last pattern
            if not has_wildcard:
                entries = [ pattern + '.rrd' ]
            files = [entry for entry in entries if isfile(join(current_dir, entry))]
            matching_files = match_entries(files, pattern + '.*')

            for base_name in matching_files + matching_subdirs:
                yield join(current_dir, base_name)


def _yield_rras(rrd_info):
    logger.debug("_yield_rras")
    for key in rrd_info:
        logger.debug("_yield_rras", key=key)
        match = search_rra_index(key)
        logger.debug("_yield_rras", match=match)
        if not match:
            continue

        yield {
            'steps': rrd_info[match.group(0)],
            'cf': rrd_info['rra[%s].cf' % match.group(1)]
        }


class RRDReader(object):
    def __init__(self, path, ds, cf, flush):
        self.fs_path = convert_fs_path(path)
        self.datasource_name = ds
        self.cf = cf
        self.flush = flush

    def get_intervals(self):
        start = time.time() - self.get_retention(self.fs_path)
        end = max( os.stat(self.fs_path).st_mtime, start )
        return IntervalSet( [Interval(start, end)] )

    def fetch(self, startTime, endTime):
        startString = time.strftime("%H:%M_%Y%m%d+%Ss", time.localtime(startTime))
        endString = time.strftime("%H:%M_%Y%m%d+%Ss", time.localtime(endTime))

        args = [self.fs_path, self.cf, '-s' + startString, '-e' + endString]
        if self.flush:
            args.append('--daemon=' + self.flush)
            args = tuple(args)

        logger.debug("FS_PATH: {0}".format(self.fs_path))
        logger.debug("ARGS: {0}".format(",".join(args)))
        (timeInfo, columns, rows) = rrdtool.fetch(*args)
        colIndex = list(columns).index(self.datasource_name)
        rows.pop() #chop off the latest value because RRD returns crazy last values sometimes
        values = (row[colIndex] for row in rows)

        return (timeInfo, values)

    @staticmethod
    def get_datasources(fs_path):
        info = rrdtool.info(convert_fs_path(fs_path))

        if 'ds' in info:
            return [datasource_name for datasource_name in info['ds']]
        else:
            ds_keys = [ key for key in info if key.startswith('ds[') ]
            datasources = set( key[3:].split(']')[0] for key in ds_keys )
            return list(datasources)

    @staticmethod
    def get_retention(fs_path):
        info = rrdtool.info(convert_fs_path(fs_path))
        if 'rra' in info:
            rras = info['rra']
        else:
            # Ugh, I like the old python-rrdtool api better..
            rra_count = max([ int(key[4]) for key in info if key.startswith('rra[') ]) + 1
            rras = [{}] * rra_count
            for i in range(rra_count):
                rras[i]['pdp_per_row'] = info['rra[%d].pdp_per_row' % i]
                rras[i]['rows'] = info['rra[%d].rows' % i]

        retention_points = 0
        for rra in rras:
            points = rra['pdp_per_row'] * rra['rows']
            if points > retention_points:
                retention_points = points

        return  retention_points * info['step']
