#encoding: utf-8

import MySQLdb
import pyodbc 
import os
import codecs

newKeywordTbl = '2017SubjectKeywords'

#计算2017年的专题库
keywordFileRelationTbl = '2017Q4keywordFileIDRelation'

#为每年的关键词和文章ID建立关系表
def checkKeywords():
    #打开mysql数据库
    kwConn= MySQLdb.connect(
                                   host='localhost',
                                   port = 3306,
                                   user='root',
                                   passwd='nopasswd',
                                   db ='rucdm',
                                   )
    kwConn.set_character_set('utf8')
    kwCur = kwConn.cursor()
    
    kfConn= MySQLdb.connect(
                                   host='localhost',
                                   port = 3306,
                                   user='root',
                                   passwd='nopasswd',
                                   db ='rucdm',
                                   )
    kfConn.set_character_set('utf8')
    kfCur = kfConn.cursor()
    
    folder = r'E:\data\subject\subjectResult'
    #主题循环，共26个
    subjectCounter = 1
    while(subjectCounter < 27):        
        subFolderID = 's'+str(subjectCounter)
        subFolder = os.path.join(folder, subFolderID)
        subFolder = os.path.join(subFolder, 'Result')
        
        keywordFileName = os.path.join(subFolder, 'keyWordFile.txt')
        kwFile = codecs.open(keywordFileName, 'r', 'utf-8')
        for eachLine in kwFile:
            ctts = eachLine.strip().split(',')
            
            readKeywords = ctts[0].split(' ')
            #对有些输入词，需要再次区分
            keyword  = '"'
            keyword += readKeywords[0]
            keyword += '"'
            print "keyword=%s" %keyword
            print "newkeywordtbl=%s" %newKeywordTbl
            print "subjectCounter=%s" %subjectCounter
            kwCur.execute('SELECT * from %s where title=%s and code=%d;'%(newKeywordTbl, keyword, subjectCounter))
            records = kwCur.fetchall()
            print "length of records = %s" %(len(records))
            if len(records) != 1:
                print 'folder=%s, kw=%s, find len=%s!'%(subFolderID, ctts[0], len(records))
                continue
            
            curRecord = records[0]
            curID = curRecord[0]
            print "curID=%s" %(curID)           
            if curRecord[4] is not None:
                subCode  = '"'
                subCode += curRecord[4]
                subCode += '"'
                print "subCode=%s" %(subCode)
                fileCounter = 1
                while( fileCounter < len(ctts) ):
                    #print 'fileId=%s'%(ctts[fileCounter])
                    fileID = int(ctts[fileCounter])
                    kfCur.execute('insert into %s (kwID,fileID,subCode,deleteFlag) values (%d,%d,%s,0);'%(keywordFileRelationTbl, curID, fileID, subCode))
                    fileCounter += 1;
                    print "fileCounter=%s" %(fileCounter)
                
                kwCur.execute('update %s set 2017q4fileNum=%d where id=%d;'%(newKeywordTbl, (fileCounter-1), curID))
            
        kfConn.commit() 
        kwConn.commit()
        
        #关闭文件
        kwFile.close()
        #换下一个文件夹
        subjectCounter+=1;
    
    print 'end'           
    return

#mark keywords: newAdd, exist, delete
#对 2017SubjectKeywords 和 r_sub 进行比较， 默认2017SubjectKeywords中为最新关键词，在 r_sub 中能找到的标为 exist，否则为 newAdd
#对已存在的关键词，沿用 r_sub 中的subCode，对newAdd，重新编制subCode
def MarkKeywords():
    #打开mysql数据库
    kwConn= MySQLdb.connect(
                                   host='localhost',
                                   port = 3306,
                                   user='root',
                                   passwd='nopasswd',
                                   db ='rucdm',
                                   )
    kwConn.set_character_set('utf8')
    kwCur = kwConn.cursor()
    
    #打开sql server服务器 这是打开.6的服务器
    #srcConn = pyodbc.connect('DRIVER={SQL Server};SERVER=XX;DATABASE=XX;UID=sa;PWD=XX')
    #srcCur = srcConn.cursor()
    srcConn = pyodbc.connect('DRIVER={SQL Server};SERVER=XX;DATABASE=XX;UID=rucreader;PWD=XX')
    srcCur = srcConn.cursor()
    
    #对每一条记录执行查找
    kwCur.execute('SELECT * from %s where subCode is Null;'%(newKeywordTbl))
    records = kwCur.fetchall()
    print 'process length: %d'%(len(records))
    for each in records:
        curId = each[0]
        
        title  = "'"
        title += each[2]
        title += "'"
        
        #在r_sub中查询是否有该关键词，注意：需要加上专题号以区分不同专题中的同一关键词
        srcCur.execute(u"select subCode from r_sub where subTitle=%s"%(title))
        findRecords = srcCur.fetchall()
        if len(findRecords) == 0:
            #print 'Err: kw=%s, findRecords=%d !'%(title, len(findRecords))
            continue;
        
        record = findRecords[0]
        startCode = each[5]
        if len(findRecords) == 1:
            record = findRecords[0]
            #检查是否是同一专题中的关键词
            checkCode = record[0][0:2]
            if checkCode != startCode:
                print 'Err: startCode=%s, checkCode=%s'%(startCode, checkCode)
                continue;
        
        #若找到记录不止一条，需要按startCode找到正确的一条
        if len(findRecords) > 1:
            findFlag = False
            #print 'War: curID=%d, kw=%s, findRecords=%d !'%(curId, title, len(findRecords))
            for eachRecord in findRecords:
                checkCode = eachRecord[0][0:2]
                if checkCode == startCode:
                    print 'subCode=%s， startCode=%s'%(eachRecord[0], startCode)  
                    record = eachRecord
                    findFlag = True
            #没找到，跳出
            if findFlag == False:
                continue;
        
        subCode  ='"'
        subCode += record[0]
        subCode +='"'
        
        print 'kw=%s, subCode=%s'%(title, subCode)
        kwCur.execute('update %s set subCode=%s where id=%d;'%(newKeywordTbl, subCode, curId))
    
    kwConn.commit();
    print 'end'


subjectIDConvort = ['01','02','03','04','05','06','07','08','09','10',
                    '11','12','13','14','15','16','17','18','19','20',
                    '21','22','23','24','25','26']
subjectKeywordCounter = [65, 31, 17, 34, 37, 30, 22, 37, 27, 22,
                         22, 61, 14,  6, 12, 22, 15, 11, 17,  8,
                         54, 23, 30, 16, 42, 20]

#生成r_sub的sql语句
def generateSQL():
    #打开mysql数据库
    kwConn= MySQLdb.connect(
                                   host='localhost',
                                   port = 3306,
                                   user='root',
                                   passwd='nopasswd',
                                   db ='rucdm',
                                   )
    kwConn.set_character_set('utf8')
    kwCur = kwConn.cursor()
    
    sqlFile = open(r'E:\data\subject\insert_r_sub.sql', 'w')
     
    subCounter = 1
    #while(subCounter < 2):
    while(subCounter < 27):
        #找出尚未编码的关键词
        kwCur.execute('select * from 2015SubjectKeywords where code=%d and (subCode is Null or subCode=\'\') and fileNum>0;'%(subCounter))
        kwRecords = kwCur.fetchall()
        kwLength = len(kwRecords)
        print 'Info: subCounter=%d, kwLength=%d'%(subCounter, kwLength)
            
        subID = subCounter - 1
        subCode = subjectIDConvort[subID]
        
        #注意: 因为两张表中
        startNum = subjectKeywordCounter[subID]
        if (startNum+kwLength) >= 100:
            print 'Err: subCounter=%d, startNum=%d, kwLength=%d'%(subCounter, startNum, kwLength)
            subCounter += 1
            continue
        
        kwcounter = 1
        for each in kwRecords:
            keyword = each[2]
            kwCode = str(startNum+kwcounter)
            if len(kwCode) < 2:
                kwCode = kwCode.zfill(2)
                
            newCode = subCode+kwCode
            if len(newCode) != 4:
                print 'Err: subCounter=%d, subCode=%d, keyword=%s, kwCode=%s'%(subCounter, subCode, keyword, kwCode)
            
            #print 'insert into r_sub (subCode, subTitle) values(\'%s\', \'%s\');'%(newCode, keyword)
            sqlFile.write('insert into r_sub (subCode, subTitle, mirstatus) values(\'%s\', \'%s\', 1);'%(newCode, keyword))
            sqlFile.write('\n')
            kwcounter += 1
            
            curId = each[0]
            kwCur.execute('update 2015SubjectKeywords set subCode=\'%s\' where id=%d;'%(newCode, curId))
        
        kwConn.commit()
        
        subCounter += 1
    
    sqlFile.close()
    print 'end'

#根据每个keyword对应的subCode，批量更新keywordFileRelationTbl表中的subCode
def updateSubCode():
    #打开mysql数据库
    kwConn= MySQLdb.connect(
                                   host='localhost',
                                   port = 3306,
                                   user='root',
                                   passwd='nopasswd',
                                   db ='rucdm',
                                   )
    kwConn.set_character_set('utf8')
    kwCur = kwConn.cursor()
    
    rtConn= MySQLdb.connect(
                                   host='localhost',
                                   port = 3306,
                                   user='root',
                                   passwd='nopasswd',
                                   db ='rucdm',
                                   )
    rtConn.set_character_set('utf8')
    rtCur = rtConn.cursor()
    
    kwCur.execute('select id, subCode from %s where 2015fileNum>0;'%(newKeywordTbl))
    records = kwCur.fetchall()
    for each in records:
        kwID = each[0]
        
        subCode  = '"'
        subCode += each[1]
        subCode += '"'
        
        rtCur.execute('update %s set subCode=%s where kwID=%d;'%(keywordFileRelationTbl, subCode, kwID))
        rtConn.commit()
    
    rtCur.close()
    kwCur.close()
    print 'end'

#生成insert r_subPaper的sql语句
def generateSQLPaper():
    #打开数据库
    rtConn= MySQLdb.connect(
                                   host='localhost',
                                   port = 3306,
                                   user='root',
                                   passwd='nopasswd',
                                   db ='rucdm',
                                   )
    rtConn.set_character_set('utf8')
    rtCur = rtConn.cursor()
    
    rtCur.execute('select subCode, fileID from %s where deleteFlag=0 and subCode is not null'%(keywordFileRelationTbl))
    records = rtCur.fetchall()
    print 'process length = %s'%(len(records))
    
    sqlFile = open(r'E:\data\subject\insert_r_subPaper.sql', 'w')
    
    for each in records:
        subCode = each[0]
        fileID = each[1]
    
        sqlFile.write('exec add_one_record_to_r_subPaper \'%s\', %d;'%(subCode, fileID))
        sqlFile.write('\n')
    
    sqlFile.close()
    rtCur.close()
    print 'end'
        
#在keywordFileRelationTbl表中,对和专题名相同的关键词，将这些关系置为deleteFlag        
def deleteSubjectFileRelation():
    #打开数据库
    rtConn= MySQLdb.connect(
                                   host='localhost',
                                   port = 3306,
                                   user='root',
                                   passwd='nopasswd',
                                   db ='rucdm',
                                   )
    rtConn.set_character_set('utf8')
    rtCur = rtConn.cursor()

    for i in range(1,27):
        subCode = '"'
        if (i < 10):
            subCode += '0'
        subCode += str(i)
        subCode += '"'
            
        rtCur.execute('update %s set deleteFlag=1 where subCode=%s'%(keywordFileRelationTbl, subCode))
   
    rtConn.commit()
    print 'end'
    

#下面为测试程序
if __name__ =="__main__":
    #在r_sub中查找已有关键词的subCode
    #MarkKeywords()
    
    #为新增的关键词产生subCode
    #generateSQL()
    
    #更新关系表中的subCode，在keywordFileRelationTbl 表中新增一栏subCode
    #updateSubCode()
    
    #将关键词和文章ID的关系存入数据表
    checkKeywords()
    
    #在keywordFileRelationTbl 表中新增一栏deleteFlag，将那些与专题名相同的关键词设为1
    deleteSubjectFileRelation()
    
    #生成更新r_subPaper的sql指令
    generateSQLPaper()
    
