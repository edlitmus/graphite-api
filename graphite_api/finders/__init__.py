import fnmatch
import os.path
import sys

def get_real_metric_path(absolute_path, metric_path):
    # Support symbolic links (real_metric_path ensures proper cache queries)
    if os.path.islink(absolute_path):
        real_fs_path = os.path.realpath(absolute_path)
        relative_fs_path = metric_path.replace('.', os.sep)
        base_fs_path = absolute_path[:-len(relative_fs_path)]
        relative_real_fs_path = real_fs_path[len(base_fs_path):]
        return fs_to_metric(relative_real_fs_path)

    return metric_path


def fs_to_metric(path):
    dirpath = os.path.dirname(path)
    filename = os.path.basename(path)
    return os.path.join(dirpath, filename.split('.')[0]).replace(os.sep, '.')


def _deduplicate(entries):
    yielded = set()
    for entry in entries:
        if entry not in yielded:
            yielded.add(entry)
            yield entry


def extract_variants(pattern):
    """Extract the pattern variants (ie. {foo,bar}baz = foobaz or barbaz)."""
    v1, v2 = pattern.find('{'), pattern.find('}')
    if v1 > -1 and v2 > v1:
        variations = pattern[v1+1:v2].split(',')
        variants = [pattern[:v1] + v + pattern[v2+1:] for v in variations]
    else:
        variants = [pattern]
    return list(_deduplicate(variants))


def match_entries(entries, pattern):
    """A drop-in replacement for fnmatch.filter that supports pattern
    variants (ie. {foo,bar}baz = foobaz or barbaz)."""
    v1, v2 = pattern.find('{'), pattern.find('}')

    if v1 > -1 and v2 > v1:
        variations = pattern[v1+1:v2].split(',')
        variants = [pattern[:v1] + v + pattern[v2+1:] for v in variations]
        matching = []

        for variant in variants:
            matching.extend(fnmatch.filter(entries, variant))

        # remove dupes without changing order
        return list(_deduplicate(matching))

    else:
        matching = fnmatch.filter(entries, pattern)
        matching.sort()
        return matching

def is_escaped_pattern(s):
    for symbol in '*?[{':
        i = s.find(symbol)
        if i > 0:
            if s[i-1] == '\\':
                return True
    return False

def find_escaped_pattern_fields(pattern_string):
    pattern_parts = pattern_string.split('.')
    for index,part in enumerate(pattern_parts):
        if is_escaped_pattern(part):
            yield index

def convert_fs_path(fs_path):
    if isinstance(fs_path, unicode):
        fs_path = fs_path.encode(sys.getfilesystemencoding())
    return os.path.realpath(fs_path)
