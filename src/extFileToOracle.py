#coding=gbk
#__author__ = 'wanghf'
import getopt
import os,sys
import time
import datetime
import itertools
import shutil

reload(sys)
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'
sys.setdefaultencoding('gbk')
from collections import OrderedDict
import decimal
import pandas as pd
import cx_Oracle as oracle



##默认当前目录,下遍历查找 形如：NAVyyymmdd.txt 文件
fileUrlDir='F:/worksparce/numpyTest/data'   #当前目录
file_prefix='RELEASE'   #文件前缀
file_suffixs=['.txt']    #读取 文件格式
version='V2.0.0.2'      #注意 必须和 文本提供的版本一致
encode='gb2312'
dates=[datetime.datetime.strftime(datetime.date.today()- datetime.timedelta(days=1),'%Y%m%d')]  #匹配读取文件的 位昨天日期


field_len_dir=OrderedDict([   #必须确保 字段排序有序，因为 程序是依照 定长来进行内容截取
                           ('id',dict(                  #基金净值日期
                                                       type=int,   #字段类型
                                                       len=2       #字段长度
                                                       )
                            ),(
                               'date', dict(               #基金净值日期
                                                             type=str,   #字段类型
                                                             len=8      #字段长度
                                                             )
                           ),(
                               'fundname', dict(           #基金名称
                                                             type=str,    #字段类型
                                                             len=50      #字段长度
                                                             )
                           ),(
                               'fundcode', dict(         #基金代码
                                                             type=str,    #字段类型
                                                             len=6        #字段长度
                                                             )
                           ),(
                               'nav', dict(      #基金单位净值
                                                              type=float,  #字段类型
                                                              len=16,      #截取的总长度
                                                              point=4       #保留的小数点位数
                                                              )
                           ),(
                               'ljNav',dict(      #基金资产总净值
                                                             type=float,    #字段类型
                                                             len=16,        #截取的总长度
                                                             point=4
                                                             )

                           ),(
                               'total_nav',dict(
                                                            type=float,
                                                            len=16,
                                                            point=4
                                                            )
                           ),(
                               'incomeunit',dict(
                                                               type=float,
                                                               len=16,
                                                               point=5
                                                               )
                           ),(
                               'incomeratio', dict(
                                                                    type=float,
                                                                    len=16,
                                                                    point=5
                                                                    )
                           ),(
                               'incomeunit_1',dict(
                                                           type=float,
                                                           len=16,
                                                           point=5
                                                           )
                           ),(
                               'incomeratio_1',dict(
                                                           type=float,
                                                           len=16,
                                                           point=5

                                                           )
                           ),(
                               'ishb',dict(
                                                                 type=str,
                                                                 len=1

                                                                 )
                           ),(
                               'totalvalue',dict(
                                                              type=float,
                                                              len=16,
                                                              point=5
                                                              )
                           ),(
                                 'hy_lj_nav',dict(
                                                                type=float,
                                                                len=16,
                                                                point=5
                                                                )
                           )
])

#写入 数据库的 连接配置信息
conf=dict(
    username='ta',
    password='oracle',
    tns='JSDC_193.86',
    table='tafundnav'
)




###将数据 插入 oracle
def dataWriteOracle(contents):

    conn=oracle.connect('%s/%s@%s' %(conf.get('username'),conf.get('password'),conf.get('tns')))
    conn.autocommit=True
    cur=conn.cursor()
    value=''
    for i in range(len(field_len_dir.keys())):
        value+=':'+str(i+1)+','
    insertSql="insert into {table}   VALUES ({value})".format(table=conf.get('table'),value=value.rstrip(','))

    cur.prepare(insertSql)

    try:
      cur.executemany(None,contents)
    except Exception as e:
        print('插入oracle出错:'+e)
    finally:
        cur.close()
        conn.commit()
        conn.close()



def analyzeFile(inputFileUrls):

    try:
        for inputfile in inputFileUrls:
            contents=[]
            reader=pd.read_table(inputfile,header=None)
            for row in reader[1:].iterrows():
                values=row[1].values[0]
                #values=''.join(values.split(' '))

                values=bytearray(values,'gbk')   #将str  转成字节进行截取

                content=[]
                for k,v in field_len_dir.items():

                   value=str(values[:v['len']])

                   if v['type']==float:

                       content.append(decimal.Decimal(value[:(len(value)-v['point'])]+'.'+value[-v['point']:]))  #str 保留小数点
                   elif k=='date':
                       dtime=time.strptime(value,'%Y%m%d')
                       content.append(datetime.date(dtime[0],dtime[1],dtime[2]))   #str 转 日期
                   else:
                       content.append(v['type'](value))
                   values=values[v['len']:]      #由于是定长处理，所以需要 将前面已经处理的内容 删除掉

                contents.append(content)
    except Exception as e:
        print("解析数据文件出错:"+e)
        sys.exit(2)
    finally:
        #删除数据文件
        for file in inputFileUrls:
          real_path=os.path.realpath(file)
          os.system('del %s' %(real_path,))


    #将数据插入 oracle数据库中
    dataWriteOracle(contents)





#解析 调用脚本的 指令
def obtainFiles(argv):
    files=[]  #读取所有的文件列表
    global  dates

    try:
        opts,args=getopt.getopt(argv,"id:")
    except getopt.GetoptError:
           print"""
               Usage: python  extFileToOracle3.py  -i [inoutFileURl]\r\n
               Opetion:
                 -i: inputFileURl    the  absolute URl  of read file. The file  like  NAVyyyymmdd.txt .
              """
           sys.exit(2)

    for opt,arg in opts:
        if opt=='-i':   #这里给的必须是全局目录
            files.extend(arg.split(','))
            return files
        if opt=='-d':  #例如： 20170802,20170803
            dates=[]
            dates.extend(arg.split(','))


    file_formats=[file_prefix+date+suffix for date,suffix in itertools.product(dates,file_suffixs)]
    print(file_formats)
    inputFileUrls=[os.path.join(fileUrlDir,url) for url in os.listdir(fileUrlDir)
                   if os.path.isfile(os.path.join(fileUrlDir,url)) and  os.path.split(url)[1] in file_formats] #
    print(inputFileUrls)

    # # #需要 将上传的文本(utf-8)  编译成 gbk
    for file in inputFileUrls:
      real_path=os.path.realpath(file)
      paths=os.path.split(real_path)

      shutil.copy(real_path,'./')

      print( sys.path[0]+'\\'+paths[1] )
      os.system('%s\\iconv.exe -f utf-8 -t gbk  %s >%s' %(sys.path[0],file,paths[1]+'_1'))  #对数据进行 转码
      os.system('del %s' %(sys.path[0]+'\\'+paths[1],))

      files.append(paths[1]+'_1')



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
