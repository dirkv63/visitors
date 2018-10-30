import apache_log_parser
from pprint import pprint
logfile = "data/other_vhosts_access.log.1"
parse_str = "%v:%p %h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\""
line_parser = apache_log_parser.make_parser(parse_str)
cnt = 0
outfile = "/home/dirk/development/python/visitors/data/parsout2.txt"
fh = open(outfile, "w")
with open(logfile, 'r') as file:
    line = file.readline()
    while line:
        cnt += 1
        # if cnt > 10:
        line_data = line_parser(line)
        pprint(line_data, fh)
        if cnt > 25:
            break
        line = file.readline()
fh.close()
