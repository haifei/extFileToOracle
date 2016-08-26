# -*- coding: utf-8 -*-
#__author__ = 'wanghf'
import getopt
import os,sys
import time
import datetime
import itertools
import shutil

reload(sys)
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'
#sys.setdefaultencoding('gbk')
from collections import OrderedDict
import decimal
import pandas as pd
import cx_Oracle as oracle



##默认当前目录,下遍历查找 形如：NAVyyymmdd.txt 文件
fileUrlDir='F:\data\yhzq'   #当前目录
file_suffixs=['.xlsx','.xls']    #读取 文件格式
encode='gb2312'


#写入 数据库的 连接配置信息
conf=dict(
    username='ta',
    password='oracle',
    tns='JSDC_193.86',
    table='yhzq_income_share_top'
)




###将数据 插入 oracle
def dataWriteOracle(contents):

    conn=oracle.connect('%s/%s@%s' %(conf.get('username'),conf.get('password'),conf.get('tns')))
    conn.autocommit=True
    cur=conn.cursor()
    value=''
    for i in range(16):
        value+=':'+str(i+1)+','
    insertSql="insert into {table}   VALUES ({value})".format(table=conf.get('table'),value=value.rstrip(','))  #(rank,fund_company,start_dt,data_dt,funds_normal,funds_csrc,total_funds_normal,total_funds_csrc,navs,total_navs,navs_per)

    print(insertSql)
    cur.prepare(insertSql)

    print(contents)
    try:
      cur.executemany(None,contents)
    except Exception as e:
        print('插入oracle出错:'+e)
    finally:
        cur.close()
        conn.commit()
        conn.close()


def to_trim(column):

    return  unicode(column).strip()

def analyzeFile(inputFileUrls):

    try:
        for inputfile in inputFileUrls:
            print(inputfile)
            # print(os.path.splitext(inputfile)[1]=='.xls')
            # if os.path.splitext(inputfile)[1] == '.xls':
            #     print('xxxxxx')
            #     inputfile = pd.ExcelFile(inputfile)
            # print(inputfile)
            contents=[]
            reader=pd.read_excel(inputfile,na_values=['NA'],parse_cols=15,converters={11:to_trim,15:to_trim}) #skiprows=[0,1,2,3,4,5,6,7,8,9]

            start=0
            end=0
            for row in reader[8:10].iterrows():
                print(row[1][0])


            for row in reader.iterrows():
                 contents.append(list(row[1]))

            # print(contents[0])
            # print(contents[1])


    except Exception as e:
        print("解析数据文件出错: %s" % (e,))
        sys.exit(2)
    # finally:
    #     #删除数据文件
    #     for file in inputFileUrls:
    #       real_path=os.path.realpath(file)
    #       os.system('del %s' %(real_path,))


    #将数据插入 oracle数据库中
    #dataWriteOracle(contents)





#解析 调用脚本的 指令
def obtainFiles(argv):
    files=[]  #读取所有的文件列表
    global  dates

    try:
        opts,args=getopt.getopt(argv,"id:")
    except getopt.GetoptError:
           print"""
               Usage: python  loadyhzq.py  -i [inoutFileURl]\r\n
               Opetion:
                 -i: inputFileURl    the  absolute URl  of read file. The file  like  xxx.xls .
              """
           sys.exit(2)

    for opt,arg in opts:
        if opt=='-i':   #这里给的必须是全局目录
            files.extend(arg.split(','))
            return files



    files=[ os.path.join(dirpath,file) for dirpath, dirnames, filenames in os.walk(fileUrlDir) for file in filenames
                    if os.path.splitext(os.path.join(dirpath,file))[1] in file_suffixs  and  not file.startswith('~$')  ]


    # # #需要 将上传的文本(utf-8)  编译成 gbk
    # for file in inputFileUrls:
    #   real_path=os.path.realpath(file)
    #   paths=os.path.split(real_path)
    #
    #   shutil.copy(real_path,'./')
    #
    #   print( sys.path[0]+'\\'+paths[1] )
    #   os.system('%s\\iconv.exe -f utf-8 -t gbk  %s >%s' %(sys.path[0],file,paths[1]+'_1'))  #对数据进行 转码
    #   os.system('del %s' %(sys.path[0]+'\\'+paths[1],))
    #
    #   files.append(paths[1]+'_1')






    if not files:
        print' file is empty'
        print """
             ERROR:  no read every file .
                  1. 将读取的文本文件，移至当前目录下
                  2.采用指令输入： -i  读取的文件，多个文件逗号分隔
             """
        sys.exit(2)

    return  files


def main(argv):



    #获取 需要读取的文件
    inputFileUrls=obtainFiles(argv)
    #解析输入 数据
    analyzeFile(inputFileUrls)






if __name__ == '__main__':
    main(sys.argv)
