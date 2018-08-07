import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')

FILE_BEGIN = "<body"
FILE_END = "/body>"
cur_html = None
with open('outfile2') as f:
    line_num = 0
    while True:
        line = f.readline()
        if FILE_BEGIN in line:
            print('{} on line {}'.format(FILE_BEGIN, line_num))
            cur_html = ""
        if FILE_END in line:
            print('{} on line {}'.format(FILE_END, line_num))
            cur_html += line

            cur_html = None
        if cur_html is not None:
            cur_html += line
        if line_num > 10000:
            break
        line_num += 1
