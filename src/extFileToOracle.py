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



##Ĭ�ϵ�ǰĿ¼,�±������� ���磺NAVyyymmdd.txt �ļ�
fileUrlDir='F:/worksparce/numpyTest/data'   #��ǰĿ¼
file_prefix='RELEASE'   #�ļ�ǰ׺
file_suffixs=['.txt']    #��ȡ �ļ���ʽ
version='V2.0.0.2'      #ע�� ����� �ı��ṩ�İ汾һ��
encode='gb2312'
dates=[datetime.datetime.strftime(datetime.date.today()- datetime.timedelta(days=1),'%Y%m%d')]  #ƥ���ȡ�ļ��� λ��������


field_len_dir=OrderedDict([   #����ȷ�� �ֶ�����������Ϊ ���������� �������������ݽ�ȡ
                           ('id',dict(                  #����ֵ����
                                                       type=int,   #�ֶ�����
                                                       len=2       #�ֶγ���
                                                       )
                            ),(
                               'date', dict(               #����ֵ����
                                                             type=str,   #�ֶ�����
                                                             len=8      #�ֶγ���
                                                             )
                           ),(
                               'fundname', dict(           #��������
                                                             type=str,    #�ֶ�����
                                                             len=50      #�ֶγ���
                                                             )
                           ),(
                               'fundcode', dict(         #�������
                                                             type=str,    #�ֶ�����
                                                             len=6        #�ֶγ���
                                                             )
                           ),(
                               'nav', dict(      #����λ��ֵ
                                                              type=float,  #�ֶ�����
                                                              len=16,      #��ȡ���ܳ���
                                                              point=4       #������С����λ��
                                                              )
                           ),(
                               'ljNav',dict(      #�����ʲ��ܾ�ֵ
                                                             type=float,    #�ֶ�����
                                                             len=16,        #��ȡ���ܳ���
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

#д�� ���ݿ�� ����������Ϣ
conf=dict(
    username='ta',
    password='oracle',
    tns='JSDC_193.86',
    table='tafundnav'
)




###������ ���� oracle
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
        print('����oracle����:'+e)
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

                values=bytearray(values,'gbk')   #��str  ת���ֽڽ��н�ȡ

                content=[]
                for k,v in field_len_dir.items():

                   value=str(values[:v['len']])

                   if v['type']==float:

                       content.append(decimal.Decimal(value[:(len(value)-v['point'])]+'.'+value[-v['point']:]))  #str ����С����
                   elif k=='date':
                       dtime=time.strptime(value,'%Y%m%d')
                       content.append(datetime.date(dtime[0],dtime[1],dtime[2]))   #str ת ����
                   else:
                       content.append(v['type'](value))
                   values=values[v['len']:]      #�����Ƕ�������������Ҫ ��ǰ���Ѿ���������� ɾ����

                contents.append(content)
    except Exception as e:
        print("���������ļ�����:"+e)
        sys.exit(2)
    finally:
        #ɾ�������ļ�
        for file in inputFileUrls:
          real_path=os.path.realpath(file)
          os.system('del %s' %(real_path,))


    #�����ݲ��� oracle���ݿ���
    dataWriteOracle(contents)





#���� ���ýű��� ָ��
def obtainFiles(argv):
    files=[]  #��ȡ���е��ļ��б�
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
        if opt=='-i':   #������ı�����ȫ��Ŀ¼
            files.extend(arg.split(','))
            return files
        if opt=='-d':  #���磺 20170802,20170803
            dates=[]
            dates.extend(arg.split(','))


    file_formats=[file_prefix+date+suffix for date,suffix in itertools.product(dates,file_suffixs)]
    print(file_formats)
    inputFileUrls=[os.path.join(fileUrlDir,url) for url in os.listdir(fileUrlDir)
                   if os.path.isfile(os.path.join(fileUrlDir,url)) and  os.path.split(url)[1] in file_formats] #
    print(inputFileUrls)

    # # #��Ҫ ���ϴ����ı�(utf-8)  ����� gbk
    for file in inputFileUrls:
      real_path=os.path.realpath(file)
      paths=os.path.split(real_path)

      shutil.copy(real_path,'./')

      print( sys.path[0]+'\\'+paths[1] )
      os.system('%s\\iconv.exe -f utf-8 -t gbk  %s >%s' %(sys.path[0],file,paths[1]+'_1'))  #�����ݽ��� ת��
      os.system('del %s' %(sys.path[0]+'\\'+paths[1],))

      files.append(paths[1]+'_1')



    if not files:
        print' file is empty'
        print """
             ERROR:  no read every file .
                  1. ����ȡ���ı��ļ���������ǰĿ¼��
                  2.����ָ�����룺 -i  ��ȡ���ļ�������ļ����ŷָ�
             """
        sys.exit(2)

    return  files


def main(argv):



    #��ȡ ��Ҫ��ȡ���ļ�
    inputFileUrls=obtainFiles(argv)
    #�������� ����
    analyzeFile(inputFileUrls)



if __name__ == '__main__':
    main(sys.argv)
