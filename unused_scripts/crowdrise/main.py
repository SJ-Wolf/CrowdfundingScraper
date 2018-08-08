from slimit import ast
from slimit.parser import Parser
from slimit.visitors import nodevisitor
from lxml import etree
from io import StringIO, BytesIO

JS_PARSER = etree.HTMLParser()
cur_file_name = 'www.crowdrise.com/becauseifeellikeacooldip'
with open(cur_file_name, 'rb') as f:
    f_read = f.read().decode('utf-8')
    f_string = StringIO(f_read)
    js_tree = etree.parse(f_string, JS_PARSER)

for i, elem in enumerate(js_tree.xpath('//script')):
    if elem.text is not None and '.donateRegistry' in elem.text:
        js_code = elem.text

JS_PARSER = Parser()
js_tree = JS_PARSER.parse(js_code)
[[(getattr(q.left, 'value', ''), getattr(q.right, 'value', '')) for q in x.initializer.properties] for x in
 nodevisitor.visit(js_tree) if isinstance(x, ast.VarDecl) if getattr(x.identifier, 'value', '') == 'args']

fields = dict()
for node in nodevisitor.visit(js_tree):
    if not isinstance(node, ast.VarDecl):
        continue
    if getattr(node.identifier, 'value', '') != 'args':
        continue
    for prop in getattr(node.initializer, 'properties', []):
        left_side_of_assignment = getattr(prop.left, 'value', '').strip("'//").strip('"')
        right_side_of_assignment = getattr(prop.right, 'value', '').strip("'//").strip('"')
        if right_side_of_assignment == 'N':
            right_side_of_assignment = False
        elif right_side_of_assignment in ('none', ''):
            right_side_of_assignment = None
        if left_side_of_assignment in fields.keys() and fields[left_side_of_assignment] != right_side_of_assignment:
            raise Exception('Existing value of {} for key {} does not equal new value of {}'.format(
                fields[left_side_of_assignment], left_side_of_assignment, right_side_of_assignment))
        fields[left_side_of_assignment] = right_side_of_assignment
# return fields

print(fields)
# fields = {getattr(node.left, 'value', ''): getattr(node.right, 'value', '') for node in nodevisitor.visit(tree) if isinstance(node, ast.Assign)}

# print fields
