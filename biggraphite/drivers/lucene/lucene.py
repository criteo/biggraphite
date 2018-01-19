import six
from biggraphite import glob_utils

##########
# Lucene #
##########

# Graphite Abstract Syntax Tree supported types
GLOBSTAR = glob_utils.Globstar
ANYSEQUENCE = glob_utils.AnySequence
ANYCHAR = glob_utils.AnyChar
VALUELIST = glob_utils.GlobExpressionWithValues
STRING = six.string_types

# Value stored in Cassandra columns component_? to mark the end of the metric name or pattern
END_MARK = "__END__"

LUCENE_FILTER = """{
    filter: {
        type: "boolean",
        must: [
            %s
        ]
    }
}"""


def translate_glob_to_lucene_filter(glob):
    """Take graphite metrics glob syntax string as input.
    Parse the glob syntax.
    Build an equivalent Apache Lucene filter using hardcoded assumptions about the index schema.
    Return an Lucene filter json string.
    """
    components = glob_utils.GraphiteGlobParser().parse(glob)
    return translate_to_lucene_filter(components)


def translate_to_lucene_filter(components):
    """Take a glob components iterable.
    Build an equivalent Apache Lucene filter using hardcoded assumptions about the index schema.
    Return an Lucene filter json string.
    """
    if any(isinstance(x, GLOBSTAR) for x in components):
        if isinstance(components[-1],GLOBSTAR):
            # No define length: match on components before the GLOBSTAR
            components.pop()
            must_list = _build_filters(components)
        else:
            # GLOBSTAR is only supported at the end of a glob syntax
            raise Exception("Metric pattern syntax only supports '%s' at the end") % GLOBSTAR
    elif ( # Parent query
        len(components) > 1
        and
        all(len(c) == 1 and isinstance(c[0],six.string_types) for c in components[:-1]) 
        and 
        isinstance(components[:-1],ANYSEQUENCE)
        ):
        parent = ""
        for component in components:
            parent += component[0] + "."
        must_list.append("""{ field:"parent", type:"match", value:"%s" }""" % parent)
    else:
        must_list = _build_filters(components)
        # Restric length by matching the END_MARK
        must_list.append("""{ field:"%s", type:"match", value:"%s" }""" % (_component_name(len(components)),END_MARK))

    return LUCENE_FILTER % ",\n\t\t".join(must_list)


def _component_name(component_index):
    return "component_%i" % component_index


def _build_filters(components):
    must_list = []
    for idx, component in enumerate(components):
        if len(component) == 0: 
            raise Exception("Illegal glob component %s in glob %s" % (component,components))
        elif len(component) == 1:
            must_list.append(_build_simple_field_constrain(idx, component[0])) 
        else: # len(component) > 1
            # Lucene prefix, wildcard & regexp are 3 different syntax.
            # Behavior and performances of underlying implementations are almost the same.
            # We therefore only use the most expressive syntax: 'regexp'
            must_list.append(_build_regex_field_constrain(idx,component))
    return filter(None,must_list)


def _build_simple_field_constrain(index, value):
    if isinstance(value,ANYSEQUENCE):
        return None # No constrain 
    elif isinstance(value,STRING):
        return """{ field:"%s", type:"match", value:"%s" }""" % (_component_name(index), value)
    elif isinstance(value,VALUELIST):
        return """{ field:"%s", type:"contains", values:["%s"] }""" % (_component_name(index), '","'.join(value.values))
    elif isinstance(value,ANYCHAR):
        # FIXME: this filter should only match 1 character long values but it doesn't
        # tried "[^\.]{1}" or "^.$" but found nothing that worked
        return """{ field:"%s", type:"regexp", value:"." }""" % _component_name(index)
    else:
        raise Exception("Unhandled type '%s'" % value)


def _build_regex_field_constrain(index, component):
    regex = ""
    for subcomponent in component:
        if isinstance(subcomponent,ANYSEQUENCE):
            regex += ".*"
        elif isinstance(subcomponent,STRING):
            regex += subcomponent
        elif isinstance(subcomponent,VALUELIST):
            regex += '('+'|'.join(subcomponent.values)+')'
        elif isinstance(subcomponent,ANYCHAR):
            # TODO: Fix AST so that AnyChar contains the actual alternatives (currently it discards the information)
            regex += '.' # This is inexact, should be '[..]'
        else:
            raise Exception("Unhandled type '%s'" % subcomponent)
    return """{ field:"%s", type:"regexp", value:"%s" }""" % (_component_name(index),regex)