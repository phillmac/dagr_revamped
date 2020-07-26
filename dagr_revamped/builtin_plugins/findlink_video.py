import sys
import json
import pickle
from dagr_revamped.plugin import DagrImportError
if not 'calmjs' in sys.modules:
    raise DagrImportError('Required package calmjs not available')
from calmjs.parse.asttypes import (
    Node as calmjs_node,
    Assign as calmjs_assign,
    Object as calmjs_obj
    )
from calmjs.parse import es5 as calmjs_es5
from calmjs.parse.walkers import Walker as calmjs_walker

def setup(manager):
    manager.register_findlink('std_video', find_video)
    return True

def find_video(current_page):
        #current_page = pickle.loads(current_page)
        try:
            script = filter_page_scripts(current_page, 'deviantART.pageData=')
            best_res = extract_nested_assign(script,['deviantART.pageData', '"film"', '"sizes"'])[-1]
            return json.loads(str(extract_nested_assign(best_res, ['"src"'])))
        except StopIteration:
            pass

def filter_page_scripts(current_page, filt):
    return next(content for content in
                (script.get_text() for script in
                        current_page.find_all('script', {'type':'text/javascript'})
                    if not script.has_attr('src'))
                if content and filt in content)

def extract_nested_assign(node, identifiers):
    if not isinstance(node, calmjs_node):
        node  = calmjs_es5(node)
    walker = calmjs_walker()
    def calmjs_do_extract(node, identifiers):
        identifier = identifiers.pop(0)
        sub_node = next(walker.filter(node, lambda n: (
            isinstance(n, calmjs_assign) and
            str(n.left) == identifier)))
        if identifiers:
            return extract_nested_assign(sub_node, identifiers)
        if isinstance(sub_node.right, calmjs_obj):
            return list(sub_node.right)
        return sub_node.right
    return calmjs_do_extract(node, identifiers)