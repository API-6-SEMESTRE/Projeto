import sys
import argparse
import os

parser=argparse.ArgumentParser(description='Log Data Scraping')
parser.add_argument('-l', '--logid', type=str, metavar='', required=True, help='Log Id (filename)')
parser.add_argument('-f', '--fileid', type=str, metavar='', required=True, help='File Id (_idFile)')
args=parser.parse_args()


def main():
    log_name = args.logid

    if not log_name.endswith('.log'):
        log_name = args.logid + '.log'
    
    file_name = os.getcwd()
    farr = file_name.split('/')
    farr2 = farr[:len(farr)-1]
    farr2.append('logs')

    file_name = '/'.join(farr2)+'/'+log_name
    
    with open(file_name, 'r') as file_obj:
        lines = file_obj.readlines()
        lines_to_scrap=[]
        for i in lines:
            if args.fileid in i:
                lines_to_scrap.append(i+'\\n')
        file_name_sailor='/'.join(farr2)+'/'+args.logid+args.fileid
        with open(file_name_sailor, 'w') as file_obj2:
            file_obj2.writelines(lines_to_scrap)

if __name__ == '__main__':
    main()

