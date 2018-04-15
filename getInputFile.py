#encoding: utf-8

'''
本程序从ruc数据库中读取全文，用于专题库选择文章的输入语料。
'''
import os
import pyodbc 
import jieba
import jieba.analyse
import codecs
import re
import shutil
import MySQLdb

from langconv import *
from gensim import corpora, models, similarities

folderName = r"E:\data\subject\file\F107"
tokenizeFolder = r"E:\data\subject\file\D424Token"

subjectSourceTbl = '2017subjectsource'

def write2file(filename,folderName, data):
    try:
        #print 'enter write2file'
        #print 'filename=%s'%(filename)        
        #print  'data[0:128]=%s'%(data[0:128])
        #print 'folderName=%s'%(folderName)
        #print 'filename=%s'%(filename)
        
        fullname = os.path.join(folderName, filename)
        #print  'fullname=%s'%(fullname)
        f = open(fullname,'wb')
        #print  'data=%s'%(data)
        
        f.writelines(data)
        f.close()
    except Exception:
        print  Exception
        print 'Err: filename=%s'%(filename.decode('gbk').encode('utf-8'))
        
    
def getArtByPdcCode(pdcCode, folderName):
    print 'getArtByPdcCode start! pdcCode=%s'%(pdcCode)
    
    #打开sql server数据库
    #artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=sa;PWD=xx')
    artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=rucreader;PWD=xx')
    
    artCur = artConn.cursor()
    
    #cttConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=sa;PWD=xx')
    cttConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=rucreader;PWD=xx')
    
    cttCur = cttConn.cursor()
    
    #查询符合pdcCode的文章ID
    #gc.disable()
    
    newCode  = "'"
    newCode += pdcCode
    newCode += "'"
    artCur.execute(u"SELECT ID,title FROM r_art WHERE pdcCode=%s ORDER BY ID asc"%(newCode))
    artRecords = artCur.fetchall()
    print 'pdcCode=%s, artNum=%d'%(pdcCode, len(artRecords))
    processNum = 0
    for eachArt in artRecords:
        queryStr = u"SELECT ctt FROM r_artCtt WHERE artID = '%d' ORDER BY ID asc" %(eachArt[0])
        temp = cttCur.execute(queryStr).fetchall()
        
        # 转换繁体到简体
        #line = Converter('zh-hans').convert(line.decode('utf-8'))
        #line = line.encode('utf-8')
        # 转换简体到繁体
        #line = Converter('zh-hant').convert(line.decode('utf-8'))
        #line = line.encode('utf-8')

        #生成标题
        #给标题添加artID，为了方便以后从标题中读出ID，将ID统一为8位数字
        title  = str(eachArt[0]).zfill(8)
        title += "_"
        titleStr = str(eachArt[1]).strip()
        titleStr = Converter('zh-hans').convert(titleStr.decode('utf-8'))
        titleStr = titleStr.encode('gbk')
        if len(titleStr) < 128:
            title += titleStr
        else:
            title += titleStr[:128]
        title += ".txt"
        
        filterChar = ['*', '\n', '"', '\\', '|', '/', '<', '>', '?', ':']
        #过滤title中不能作为文件标题出现的字符
        titleName = ''
        for eachChar in title:
            if eachChar in filterChar:
                continue
            else:
                titleName += eachChar

        #生成内容
        content = u""
        try:
            for i in temp:
                i = i[0].decode('gbk','ignore').strip()
                content = content + i
                
            #繁简转换
            contentStr = Converter('zh-hans').convert(content)
            contentStr = contentStr.encode('utf-8')
                    
        except UnicodeDecodeError:
            print 'Err: i=', i
            continue
        #yield content
        #write2file(title.encode('gbk'), content.encode('utf-8'))  
        #print  'titleName=%s'%(titleName)
        write2file(titleName, folderName, contentStr)  
        
        processNum += 1
        if processNum%100 == 0:
            print 'processNum=%s'%(processNum)
          
    #gc.enable()
    print 'getArtByPdcCode end! pdcCode=%s'%(pdcCode)
    artCur.close()
    cttCur.close()
    
#该函数与getArtByPdcCode的区别就是不做繁简转换
def getArtByPdcCode2(pdcCode, folderName):
    #print 'getArtByPdcCode start! pdcCode=%s'%(pdcCode)
    
    #打开sql server数据库
    #artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=sa;PWD=xx')
    artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=rucreader;PWD=xx')
    
    artCur = artConn.cursor()
    
    #cttConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=sa;PWD=xx')
    cttConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=rucreader;PWD=xx')
    
    cttCur = cttConn.cursor()
    
    #查询符合pdcCode的文章ID
    #gc.disable()
    
    newCode  = "'"
    newCode += pdcCode
    newCode += "'"
    artCur.execute(u"SELECT ID,title FROM r_art WHERE pdcCode=%s ORDER BY ID asc"%(newCode))
    artRecords = artCur.fetchall()
    print 'pdcCode=%s, artNum=%d'%(pdcCode, len(artRecords))
    processNum = 0
    for eachArt in artRecords:
        queryStr = u"SELECT ctt FROM r_artCtt WHERE artID = '%d' ORDER BY ID asc" %(eachArt[0])
        temp = cttCur.execute(queryStr).fetchall()
        
        #生成标题
        #给标题添加artID，为了方便以后从标题中读出ID，将ID统一为8位数字
        title  = u''
        title += str(eachArt[0]).zfill(8)
        title += "_"
        
        titleStr = u""+str(eachArt[1])
        if len(titleStr) > 128:
            titleStr = titleStr[:120]
            
        #先用正则表达式过滤非汉字字符
        groups = re.findall(u'[\u4E00-\u9FA5]+', titleStr) 
        #print 'len=%s'%(len(groups))
        titleStr = u''
        for each in groups:
            titleStr += each
        
        #再过滤一遍不适合作为标题的字符
        #filterChar = ['*', '\n', '"', '\\', '|', '/', '<', '>', '?', ':', '\t', '\f', '\r',
        #              '\v', u'\ue524', u'\ue012', u'\ue0f3',u'\ue0b5', u'\ue003', u'\ue5e4']
        filterChar = ['*', '\n', '"', '\\', '|', '/', '<', '>', '?', ':', '\t', '\f', '\r', '\v']
        #过滤title中不能作为文件标题出现的字符
        for eachChar in filterChar:
            #title.replace(eachChar, '_');
            if titleStr.find(eachChar) >= 0:
                #print 'title=%s, eachChar=%s'%(title, eachChar)
                ctts = titleStr.split(eachChar)
                newTitle = u''
                for eachCtt in ctts:
                    newTitle += eachCtt
                #更新titleStr，以防有多个不合适字符
                titleStr = newTitle
                    
        title += titleStr
        title += ".txt"
        title = title.encode('utf-8')
            
        print 'title=%s, id=%s'%(title, eachArt[0])
        
        #生成内容
        content = u""
        try:
            for i in temp:
                i = i[0].decode('gbk','ignore').strip()
                content = content + i  
            
            content = content.encode('utf-8')     
            #title = title.encode('gbk')  
              
            print 'title=%s'%(title)
        
        except UnicodeDecodeError:
            print 'Err: pdcCode=%s, artId=%s'%(pdcCode, eachArt[0])
            continue
        
        #yield content
        write2file(title, folderName, content)  
        #print  'titleName=%s'%(titleName)  
        
        processNum += 1
        #if processNum%1000 == 0:
        #    print 'processNum=%s'%(processNum)
          
    #gc.enable()
    #print 'getArtByPdcCode end! pdcCode=%s'%(pdcCode)
    artCur.close()
    cttCur.close()

#将所有论文全文按pdcCode分类下载下来
def getAllArt(folder):
    #打开sql server数据库
    #artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=sa;PWD=xx')
    artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=rucreader;PWD=xx')
    
    artCur = artConn.cursor()
    
    #查询所有的pdcCode
    counter = 0
    artCur.execute(u"SELECT pdcCode, count(pdcCode) from r_art GROUP BY pdcCode;")
    pdcRecords = artCur.fetchall()
    print 'kind of pdcCodes: %s'%(len(pdcRecords))
    for each in pdcRecords:
        pdcCode = each[0]
        fileNum = each[1]
        
        #判断对应的Folder是否已建立
        pdcFolder = os.path.join(folder, pdcCode)
        flag = os.path.exists(pdcFolder)
        if flag == False:
            os.mkdir(pdcFolder) 
        else:
            continue;
        
        #下载这一类别的文章
        getArtByPdcCode2(pdcCode, pdcFolder)
        
        counter += fileNum
        print 'pdcCode=%s end, counter=%s'%(pdcCode, counter)
    
    print 'process end!'
    artCur.close()
    
#检查输出每个文件夹下的文章数是否与数据库中查询得到的相符    
def checkAllArt(folder):
    #打开sql server数据库
    #artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=sa;PWD=xx')
    artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=rucreader;PWD=xx')
    
    artCur = artConn.cursor()
    
    #查询所有的pdcCode
    #counter = 0
    artCur.execute(u"SELECT pdcCode, count(pdcCode) from r_art GROUP BY pdcCode;")
    pdcRecords = artCur.fetchall()
    print 'kind of pdcCodes: %s'%(len(pdcRecords))
    for each in pdcRecords:
        pdcCode = each[0]
        fileNum = each[1]
        
        #判断对应的Folder是否已建立
        pdcFolder = os.path.join(folder, pdcCode)
        flag = os.path.exists(pdcFolder)
        if flag == False:
            print 'Err: %s has no folder!'%(pdcCode)
            continue;
        
        files = os.listdir(pdcFolder)
        downloadFileNum = len(files)
        
        if downloadFileNum != fileNum:
            print 'pdcCode=%s, downloadFileNum=%s, fileNum=%s'%(pdcCode, downloadFileNum, fileNum)
            
    print 'check end!'

#该函数与getArtByPdcCode2的区别是按年份下载文件
def getArtByPdcCodeByYear(pdcCode,pdcNo,folderName,year):
    #print 'getArtByPdcCode start! pdcCode=%s'%(pdcCode)
    
    #打开sql server数据库
    #artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=sa;PWD=xx')
    #登录sqlserver数据库（两个变量一样的意义，求解)
    artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=rucreader;PWD=xx')
    artCur = artConn.cursor()
    
    #cttConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=sa;PWD=xx')
    cttConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=xx;UID=rucreader;PWD=xx') 
    cttCur = cttConn.cursor()
    
    #查询符合pdcCode的文章ID
    #gc.disable()
    
    #分别把传入参数year和pdccode保存为字符串变量
    newYear  = "'"
    newYear += year
    newYear += "'"
    newCode  = "'"
    newCode += pdcCode
    newCode += "'"
    #打印字符串变量newCode信息
    #print "newCode=%s"%(newCode)
    newNo = str(pdcNo).split(',')
    #打印字符串变量newNo信息
    #print "newNo=%s"%(newNo)
    
    for each in newNo:
        #在sqlserver数据库中选出符合pdcCode和pdcNo一一对应关系的行的ID和title，以ID升序排列
        artCur.execute(u"SELECT ID,title FROM r_art WHERE pdcCode=%s and pdcNo=%s and pdcYear=%s ORDER BY ID asc"%(newCode, each, newYear))
        #把上述命令执行结果二维数组保存到变量artRecords
        artRecords = artCur.fetchall()
        #打印此pdcNo对应的pdccode的信息和二维数组的长度
        #print 'pdcCode=%s, artNum=%d'%(pdcCode, len(artRecords))
        
        #从表artctt中获取文章内容保存到对应名字的文本文件
        #设置计数器
        processNum = 0
        #执行等于二维数组artrecords长度数次数循环
        for eachArt in artRecords:
            #在表artctt中选出满足二维数组artrecords的id的行的ctt
            queryStr = u"SELECT ctt FROM r_artCtt WHERE artID = '%d' ORDER BY ID asc" %(eachArt[0])
            #把上述命令执行结果吧哦窜到temp变量
            temp = cttCur.execute(queryStr).fetchall()
            
            #生成标题
            #把artID保存为字符串变量title
            #给标题添加artID，为了方便以后从标题中读出ID，将ID统一为8位数字            
            title  = u''
            title += str(eachArt[0]).zfill(8)
            title += "_"
            
            #把表art中的title保存为Unicode对象titlestr
            titleStr = u""+str(eachArt[1])
            #如果titlestr长度过长，仅保留前120字节内容
            if len(titleStr) > 128:
                titleStr = titleStr[:120]
                
            #先用正则表达式过滤非汉字字符
            #在titlestr中找出符合[\u4E00-\u9FA5]+条件的字符串保存到groups
            groups = re.findall(u'[\u4E00-\u9FA5]+', titleStr) 
            #print 'len=%s'%(len(groups))
            titleStr = u''
            for each in groups:
                #每个title分别保存到titleStr
                titleStr += each
            
            #再过滤一遍不适合作为标题的字符
            #filterChar = ['*', '\n', '"', '\\', '|', '/', '<', '>', '?', ':', '\t', '\f', '\r',
            #              '\v', u'\ue524', u'\ue012', u'\ue0f3',u'\ue0b5', u'\ue003', u'\ue5e4']
            filterChar = ['*', '\n', '"', '\\', '|', '/', '<', '>', '?', ':', '\t', '\f', '\r', '\v']
            #过滤title中不能作为文件标题出现的字符
            for eachChar in filterChar:
                #title.replace(eachChar, '_');
                if titleStr.find(eachChar) >= 0:
                    #print 'title=%s, eachChar=%s'%(title, eachChar)
                    ctts = titleStr.split(eachChar)
                    newTitle = u''
                    for eachCtt in ctts:
                        newTitle += eachCtt
                    #更新titleStr，以防有多个不合适字符
                    titleStr = newTitle
                        
            title += titleStr
            title += ".txt"
            #title = title.encode('utf-8')
                
            #print 'title=%s, id=%s'%(title, eachArt[0])
            
            #生成内容
            content = u""
            try:
                #temp是个一维数组，需要用i[0]表示其中的值么
                for i in temp:
                    #print 'i=%s'%(i[0])                
                    #i = i[0].decode('gbk','ignore').strip()
                    content = content + i[0]  
                
                #content = content.encode('utf-8')     
                #title = title.encode('gbk')  
                
                #print 'title=%s'%(title)   
                
            except UnicodeDecodeError:
                print 'Err: pdcCode=%s, artId=%s'%(pdcCode, eachArt[0])
                continue
            
            #yield content
            #把内容写入到对应文件夹
            write2file(title, folderName, content)    
            
            #计数器加1
            processNum += 1
        
            #if processNum%1000 == 0:
            #    print 'processNum=%s'%(processNum)
          
    #gc.enable()
    #print 'getArtByPdcCode end! pdcCode=%s'%(pdcCode)
    #关闭数据库连接
    artCur.close()
    cttCur.close()
    
#将所有论文全文按pdcCode分类下载下来，与getAllArt的区别是按指定年份下载
#将数据来源从.6服务器改成从线上xx上下载
def getAllArtByYear(folder, year):
    #打开mysql数据库
    #登录数据库
    kwConn= MySQLdb.connect(
                               host='localhost',
                               port = 3306,
                               user='root',
                               passwd='nopasswd',
                               db ='rucdm',
                           )
    #设置执行命令编码类型
    kwConn.set_character_set('utf8')
    kwCur = kwConn.cursor()
    #从2017Q1pdcNo中选出所有行，以pdccode升序排列 
    kwCur.execute(u"SELECT * FROM 2017Q4pdcNo ORDER BY pdcCode asc")
    #取出上述命令执行结果二维数组pdcCode和pdcNo保存到变量pdcRecords
    pdcRecords = kwCur.fetchall()
    #打印出变量pdcRecords的长度
    print 'pdcRecords=%s, kind of pdcCodes:%s'%(pdcRecords, len(pdcRecords))
    #把二维数组pdcRecords中的pdcCode保存到pdcCode,把pdcNo保存到pdcNo,二者均为一维数组,循环执行次数=pdcCode种类数
    for each in pdcRecords:
        pdcCode = each[0]
        #print 'pdcCode=%s'%(pdcCode)
        pdcNo = each[1]
        #print 'pdcNo=%s'%(pdcNo)
        
        #判断对应的Folder是否已建立
        #把路径 数据存放根目录/期刊号文件夹 保存到变量pdcFolder
        pdcFolder = os.path.join(folder, pdcCode)
        #判断上述路径是否已建立，结果保存到flag,值为True或False
        flag = os.path.exists(pdcFolder)
        #若未建立上述路径，建立；否则继续执行
        if flag == False:
            os.mkdir(pdcFolder)
            print '%s has been established!'%(pdcFolder) 
        else:
            continue; 
        
        #下载这一类别的一季度的文章
        print 'Downloading files belong to %s' %(pdcCode)
        getArtByPdcCodeByYear(pdcCode,pdcNo,pdcFolder,year)
        #打印执行完以上步骤pdccode和pdcno的信息
        #print 'pdcCode=%s end, pdcNo=%s'%(pdcCode, pdcNo)
    #所有循环执行结束后打印结束信息
    print 'process end!'
    #关闭数据库连接
    kwCur.close()

#检查输出每个文件夹下的文章数是否与数据库中查询得到的相符    
def checkAllArtByYear(folder, year):
    #打开sql server数据库
    #artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=ruc;UID=sa;PWD=xx')
    artConn = pyodbc.connect('DRIVER={SQL Server};SERVER=xx;DATABASE=ruc;UID=rucreader;PWD=xx')
    artCur = artConn.cursor()
    
    #查询所有的pdcCode
    #counter = 0
    newYear  = "'"
    newYear += year
    newYear += "'"
    artCur.execute(u"SELECT pdcCode, count(pdcCode) from r_art where pdcYear=%s GROUP BY pdcCode;"%(newYear))
    pdcRecords = artCur.fetchall()
    print 'kind of pdcCodes: %s'%(len(pdcRecords))
    for each in pdcRecords:
        pdcCode = each[0]
        fileNum = each[1]
        
        #判断对应的Folder是否已建立
        pdcFolder = os.path.join(folder, pdcCode)
        flag = os.path.exists(pdcFolder)
        if flag == False:
            print 'Err: %s has no folder!'%(pdcCode)
            continue;
        
        files = os.listdir(pdcFolder)
        downloadFileNum = len(files)
        
        if downloadFileNum != fileNum:
            print 'pdcCode=%s, downloadFileNum=%s, fileNum=%s'%(pdcCode, downloadFileNum, fileNum)
            
    print 'check end!'
        
#结巴增加新词,输入参数为新词表所在的文件目录
def  addNewWord(inputFolder):
    
    files = os.listdir(inputFolder) 
    inFiles = [os.path.join(inputFolder, f) for f in files]
    wordCounter = 0
    for each in inFiles:
        wordin = codecs.open(each, 'r', 'utf-8');
        for line in wordin:
            newWord = line.strip()
            newWords = newWord.split(' ')
            #if len(newWords) > 1:
            #    print newWords
            for eachWord in newWords:
                jieba.add_word(eachWord)  
            wordCounter += 1      
        wordin.close()
    print  '%s words have been added into jieba word dict!'%(wordCounter)
    
#分词
def tokenizeOneFile(inputName, outputName, stop_words):
    fin = codecs.open(inputName, 'r', 'utf-8');
    fout = codecs.open(outputName, 'w', 'utf-8')
    count = 0
    #print 'inputName=%s, outputName=%s'%(inputName, outputName)
    
    for line in fin:
        lineWord = jieba.cut(line, cut_all=False)
        
        #去掉标点符号和停止词
        validWords = []
        for eachword in lineWord:
            if eachword not in stop_words:
                n = re.search(ur'[\u4e00-\u9fa5]+|[A-Za-z]+', eachword)
                if n is not None:
                    validWords.append(n.group())
                
        words = " ".join(validWords)
        fout.write(words + ' ')
        
        count += 1
        if count%1000 == 0:
            fout.flush()
            
    fin.close()
    fout.close()
    
#得到停用词表
def getStopWords(stopWordFile):
    fstop = codecs.open(stopWordFile, 'r', 'utf-8')
    stop_word_line = fstop.readline()
    stop_words = stop_word_line.split(' ')  
    fstop.close()
    
    return stop_words
    print 'length of stop words is %s'%(len(stop_words))
    
#将inputFolder文件夹下面的文件挨个分词，存入outputFolder
def tokenByJieba(inputFolder, outputFolder):

    #获取文件夹下的所有文件
    files = os.listdir(inputFolder) 
    inFiles = [os.path.join(inputFolder, f) for f in files] # add path to each file
    print 'Total %s files'%(len(inFiles))
    
    #建停用词表，这里用常用停用词
    stopWordFile = r"E:\data\subject\stopWord\zhStopWords.txt"
    stop_words = getStopWords(stopWordFile)
               
    fileCounter = 0
    for each in inFiles:
        #生成输出文件名称
        folder, fname = os.path.split(each)
        outFilename = os.path.join(outputFolder, fname)
        tokenizeOneFile(each, outFilename, stop_words)
        fileCounter += 1
        if fileCounter%100 == 0:
            print '%s files have been processed!'%(fileCounter)
    
    print 'Totally process %s files'%(fileCounter)
    
   
#更新专题的来源文件，每个专题有固定的来源刊，
def updateSubjectInput(baseFolder, qkInFolder):
    #打开mysql数据库
    qkConn= MySQLdb.connect(
                                   host='localhost',
                                   port = 3306,
                                   user='root',
                                   passwd='nopasswd',
                                   db ='rucdm',
                                   )
    qkConn.set_character_set('utf8')
    qkCur = qkConn.cursor()
    
    #从数据库中读取一个专题对应的期刊
    qkCur.execute('SELECT id,pdcCode from %s;'%(subjectSourceTbl))
    subjects = qkCur.fetchall()
    for each in subjects:
        #得到来源刊列表
        qkCodes = each[1].split(u'、')

        #清空文件夹，先用rmtree命令删除文件夹，再重建文件夹
        sId = 's' + str(each[0])
        sFolder = os.path.join(baseFolder, sId)
        inFolder = os.path.join(sFolder, 'tokenInput')
        shutil.rmtree(inFolder)
        os.mkdir(inFolder)
    
        #复制来源刊文件夹
        for eachQk in qkCodes:
            qkSrcFolder = os.path.join(qkInFolder, eachQk)
            qkDstFolder = os.path.join(inFolder, eachQk)
            shutil.copytree(qkSrcFolder, qkDstFolder, False)
            
        print 'Processed %d subject!'%(each[0])
    
    qkConn.close()
    
    print 'process end!'
    return;

#将26个专题的来源文件进行分词，存入对应的分词后文件夹
def updateSubjectToken(baseFolder):
    
    #给jieba分词加入新词
    newWordFolder = r"E:\data\subject\wordDict\subjectKeywords"
    addNewWord(newWordFolder)
    
    print 'Start to process!'
    for i in range(1,27):
        
        #定位到每个专题的输入输出文件夹
        sId = 's' + str(i)
        sFolder = os.path.join(baseFolder, sId)
        inFolder = os.path.join(sFolder, 'tokenInput')
        outFolder = os.path.join(sFolder, 'tokenOutput')
        
        #清空token输出文件夹
        shutil.rmtree(outFolder)
        os.mkdir(outFolder)
        
        #获取输入文件夹列表
        subFolders = os.listdir(inFolder)
        for each in subFolders:
            subInFolder = os.path.join(inFolder, each)
            subOutFolder = os.path.join(outFolder, each+'Token')
            os.mkdir(subOutFolder)
            tokenByJieba(subInFolder, subOutFolder)
            print '%d subject: %s pdcCode processed!'%(i, each)
            
            #test
            #break
        #test
        #break
    
    print 'Process end!'
    
    return;



#下面为测试程序
if __name__ =="__main__":
    #读入刊号为 pcdCode 的文章
    #pcdCode = 'F107'
    #folderName = r"E:\data\subject\file\F107"
    #getArtByPdcCode(pcdCode, folderName)
    
    #将文件夹下的全部文章切词
    #inputFolder = r'E:\data\subject\file\D424'
    #outputFolder = r'E:\data\subject\file\D424Token'
    #tokenByJieba(inputFolder, outputFolder)
    
    #下载全部文件
    #artFolder = r'E:\data\rucArt'
    #getAllArt(artFolder)
    
    #检查下载文件
    #artFolder = r'E:\data\rucArt'
    #checkAllArt(artFolder)
    
    #下载2015年全部文件
    #artFolder = r'E:\data\rucArt2015_2'
    #getAllArtByYear(artFolder, '2015')
    #checkAllArtByYear(artFolder, '2015')
    
    #给jieba分词加入新词
    #newWordFolder = r"E:\data\subject\wordDict\subjectKeywords"
    #addNewWord(newWordFolder)
    
    #将文件夹下的全部文章切词
    #D424是港澳台专题，有繁简转换，用这个目录下的
    #inputFolder = r'E:\data\subject\file\D424'
    #新专题用全库，旧专题用2015年新的文章
    #inputFolder = r'E:\data\rucArt2015_2\V3'
    #outputFolder = r'E:\data\subject\subjectResult\s21\V3Token'
    #tokenByJieba(inputFolder, outputFolder)
    
    '''    #给双层文件夹下的文件分词
    inputFolder = r'E:\data\subject\subjectResult\s1\tokenInput'
    outputFolder = r'E:\data\subject\subjectResult\s1\tokenOutput'
    subFolders = os.listdir(inputFolder)
    for eachFolder in subFolders:
        print eachFolder
        outFolderName = eachFolder+'Token'
        fullInFolder = os.path.join(inputFolder, eachFolder)
        fullOutFolder = os.path.join(outputFolder, outFolderName)
        if os.path.exists(fullOutFolder):
            #已分词，跳出
            continue
        else:
            os.mkdir(fullOutFolder)
        tokenByJieba(fullInFolder, fullOutFolder)
    '''
    
    #test
    #eachword=u'香港'
    #eachword=u'CEPA'
    #n = re.search(ur'[\u4e00-\u9fa5]+|[A-Za-z]+', eachword)
    #if n is not None:
    #    print n.group()
    
    #下载2017年全部文件
    #artFolder = r'E:\data\rucArt2017'
    #getAllArtByYear(artFolder, '2017')
    #checkAllArtByYear(artFolder, '2017')
    
    #更新专题的来源文件
    #subjectFolder = r'E:\data\subject\subjectResult'
    #qkInFolder = r'E:\data\rucArt2017'
    #updateSubjectInput(subjectFolder, qkInFolder)
    
    #为更新后的文件做分词
    subjectFolder = r'E:\data\subject\subjectResult'
    updateSubjectToken(subjectFolder)
    
