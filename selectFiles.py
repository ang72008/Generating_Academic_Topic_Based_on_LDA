#encoding: utf-8

'''
本程序做文章相似度计算实验
'''

import os
import sys
import codecs
import logging
import shutil
import MySQLdb
import pyodbc 
from gensim import corpora, models, similarities


#通用函数，清空或者创建文件夹
def cleanDir(folder):
    
    if os.path.exists(folder):
        shutil.rmtree(folder)
        
    os.mkdir(folder)
    return;

#将指定文件夹下的所有文章内容读入生成一个列表，一篇文章内容为一行，输入文章已经过分词和过滤处理
#input： 输入文件所在的文件夹
#output: 二维列表，包括多个文章，每个文章为一个词列表
def getTextsFromFolder(foldername):
    
    files = os.listdir(foldername) 
    texts,fnames,folders = [],[],[]
    for each in files:
        fnames.append(each)
        fullname = os.path.join(foldername, each)
        fin = codecs.open(fullname, 'r', 'utf-8')
        content = fin.readline()
        words = content.strip().split(u' ')
        #print 'len(words)=%s'%(len(words))
        texts.append(words)
        folders.append(foldername)
        
    #print 'read texts end!'
    return (folders,fnames,texts)

#将多个指定文件夹下的所有文章内容读入生成一个列表，输入参数为一个文件夹的列表
def  getTextsFromFolders(foldernames):
    print 'Read files! folder num=%s'%(len(foldernames))
    
    texts,fnames,folders = [],[],[]
    for eachFolder in foldernames:
        eachFolders, eachFnames, eachTexts = getTextsFromFolder(eachFolder)
        texts.extend(eachTexts)
        fnames.extend(eachFnames)
        folders.extend(eachFolders)
        
    #print 'read texts from folders end!'
    return (folders, fnames, texts)
        
#生成bag-of-words
def genDict(texts):
    #生成bag-of-words
    dictionary = corpora.Dictionary(texts)     #Dictionary 输入是一个二维列表
    #print 'dictionary: ', dictionary
    #print 'dictionary.token2id: ', dictionary.token2id
    #for each in dictionary.token2id:
    #    print each
    
    print 'generate dict end!'
    return dictionary
        
#LSI模型计算        
def calLSIandIndex(dictionary, texts):
    #文章转化为向量
    corpus = [dictionary.doc2bow(text) for text in texts]
    #基于这些训练文档计算一个TF-IDF
    tfidf = models.TfidfModel(corpus)
    #基于这个TF-IDF模型，将上述用词频表示的文档向量表示为用tf-idf值表示的文档向量
    corpus_tfidf = tfidf[corpus]
    
    #用tf-idf值表示的文档向量，训练LSI模型
    lsi = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=500)
    
    #用lsi模型将文档映射到topic空间中   
    corpus_lsi = lsi[corpus_tfidf]
    #for doc in corpus_lsi:
    #    print doc
    
    #建立文档基于LSI模型的索引
    #similarities.MatrixSimilarity这个类只适用于所有语料可以放入内存的情况，similarities.Similarity类扩展性更好
    index = similarities.MatrixSimilarity(lsi[corpus])

    print 'cal LSI and index end!'
    
    return lsi, index

#LDA模型计算        
def calLDAandIndex(dictionary, texts):
    print 'enter calLDAandIndex...'
    
    #文章转化为向量
    corpus = [dictionary.doc2bow(text) for text in texts]
    #基于这些训练文档计算TF-IDF
    tfidf = models.TfidfModel(corpus)
    #基于这个TF-IDF模型，将上述用词频表示的文档向量表示为用tf-idf值表示的文档向量
    corpus_tfidf = tfidf[corpus]
    
    #用tf-idf值表示的文档向量，训练LDA模型
    lda = models.LdaModel(corpus_tfidf, id2word=dictionary, num_topics=200)
    lda.show_topics()
    
    #用lda模型将文档映射到topic空间中   
    corpus_lda = lda[corpus_tfidf]
    #for doc in corpus_lsi:
        #print doc
    
    #建立文档基于LSI模型的索引
    #similarities.MatrixSimilarity这个类只适用于所有语料可以放入内存的情况，similarities.Similarity类扩展性更好
    index = similarities.MatrixSimilarity(lda[corpus])

    print 'cal LDA and index end!'
    
    return lda, index

#查询指定文件的相似度
def querybyMolIndex(mod, index, query_bow):
    #用之前训练好的模型将其映射到topic空间
    query_lsi = mod[query_bow]
    #print 'query_lsi: ', query_lsi
    
    #计算它和index中doc的余弦相似度：
    sims = index[query_lsi]
    
    #然后按相似度进行排序：
    sort_sims = sorted(enumerate(sims), key=lambda item: -item[1])
    return sort_sims

#读入文章内容，目前默认所有文章只有一行内容
def getTextsFromFile(fname):
    fin = codecs.open(fname, 'r', 'utf-8')
    #fin = open(fname, 'r')
    texts = []
    for eachLine in fin:
        words = eachLine.strip().split(' ')
        for eachWord in words:
            texts.append(eachWord)       
    fin.close()   
    return texts

#对指定文件夹下的文件进行处理，计算和专题文件的相似性     
def docTest(inFolders, subFolder, keywordFile):   
    
    #处理docTest下的所有文章，生成所需texts
    #inputFolder = r'E:\data\subject\file\subject26Input'
    #fnames, texts = getTextsFromFolder(inputFolder)
    
    #得到输入文件，输入文件可能从多个文件夹下
    (folders, fnames, texts) = getTextsFromFolders(inFolders)

    print 'end of read files! length of file = %s'%(len(fnames))
    
    #建立词典并保存，即bag-of-words
    #middlefolder = r'E:\data\subject\file\Dict&Model'
    #dictName = 'shortTest.dict'
    #dictName = 'longTest.dict'
    dictionary = genDict(texts)
    #dictionary.save(os.path.join(middlefolder, dictName))
    #dictionary = corpora.Dictionary.load(os.path.join(middlefolder, dictName))
    
    #建立lsi模型并计算所有文章索引
    #middlefolder = r'E:\data\subject\file\Dict&Model'
    #lsiModeName = 'shortLSI.model'
    #lsiIdxName = 'shortLSI.index'
    #lsiModeName = 'longLSI.model'
    #lsiIdxName = 'longLSI.index'
    mod, index = calLSIandIndex(dictionary, texts)
    #mod.save(os.path.join(middlefolder, lsiModeName))
    #index.save(os.path.join(middlefolder, lsiIdxName))
    #mod = models.LsiModel.load(os.path.join(middlefolder, lsiModeName))
    #index = similarities.MatrixSimilarity.load(os.path.join(middlefolder, lsiIdxName))
    
    #建立LDA模型并计算所有文章索引
    #LDAModeName = 'shortLDA.model'
    #LDAIdxName = 'shortLDA.index'
    #LDAModeName = 'longLDA.model'
    #LDAIdxName = 'longLDA.index'
    #mod, index = calLDAandIndex(dictionary, texts);
    #mod.save(os.path.join(middlefolder, LDAModeName))
    #index.save(os.path.join(middlefolder, LDAIdxName))
    #mod = models.LdaModel.load(os.path.join(middlefolder, LDAModeName))
    #index = similarities.MatrixSimilarity.load(os.path.join(middlefolder, LDAIdxName))
    
    #读入专题关键词表
    #fname = r'E:\data\subject\subjectResult\s25\subject25.txt'
    fname = os.path.join(subFolder, keywordFile)
    query_words = getTextsFromFile(fname);
    #query_words = ['特首']
    #print 'len of query_words=%s'%(len(query_words))
    query_bow = dictionary.doc2bow(query_words)
    #print 'query_bow=', query_bow
    print 'len of query_bow=%s'%(len(query_bow))
    
    #print 'write result...'
    
    sort_sims = querybyMolIndex(mod, index, query_bow)
    sortCounter = 0
    #resultFile = r'E:\data\subject\subjectResult\s25\LSI_500.txt'
    fileName = 'LSI_500.txt'
    fullName = os.path.join(subFolder, fileName)
    sortFile = open(fullName, 'w')
     
    #selectFolder = r'E:\data\subject\subjectResult\s25\Files'
    selectFolder = os.path.join(subFolder, 'selectFiles')
    cleanDir(selectFolder)
    for item in sort_sims:
        if item[1] < 0.04:
            break
            
        filename = fnames[item[0]].decode('gbk').encode('utf-8')
        #print 'LSI: index=%s, filename is %s, sims=%s'%(item[0], filename, item[1])
        sortFile.write('LSI: index=%s, filename is %s, sims=%s'%(item[0], filename, item[1]))
        sortFile.write('\n')
        
        selectFile = os.path.join(folders[item[0]], fnames[item[0]])
        copyFile = os.path.join(selectFolder, fnames[item[0]])
        #print 'src=%s, des=%s'%(selectFile, copyFile)
        shutil.copyfile(selectFile, copyFile)
        
        sortCounter += 1
        #首先输出1000篇文章
        
    print 'write result end!sortCounter=%d'%(sortCounter)
    return sortCounter;
    
    '''
    #读入专题关键词表,每个关键词为一行
    fname = r'E:\data\subject\wordDict\subjectKeywords\subject26.txt'
    fkeywords = open(fname, 'r')
    for eachLine in fkeywords:
        keyword = eachLine.strip()
        query_words = []
        query_words.append(keyword)
        query_bow = dictionary.doc2bow(query_words)
    
        print 'keyword=%s, write result...'%(keyword)
    
        sort_sims = querybyMolIndex(mod, index, query_bow)
        sortCounter = 0
        resultFolder = r'E:\data\subject\file\subjectResult\subject26'
        resultFile = keyword.decode('utf-8').encode('gbk') + '_s26_LSI_sort1000.txt'
        fullName = os.path.join(resultFolder, resultFile)
        sortFile = open(fullName, 'w')
        for item in sort_sims:
            filename = fnames[item[0]].decode('gbk').encode('utf-8')
            #print 'LSI: index=%s, filename is %s, sims=%s'%(item[0], filename, item[1])
            sortFile.write('LSI: index=%s, filename is %s, sims=%s'%(item[0], filename, item[1]))
            sortFile.write('\n')
            sortCounter += 1
            #首先输出1000篇文章
            if sortCounter >= 1000:
                break;
        print 'write result end!'
        sortFile.close()
    print 'keywords process end!'
    fkeywords.close()
    '''
    
#为每个关键词匹配文章
def wordTest(subFolder, keywordFile):
    #处理docTest下的所有文章，生成所需texts
    #inputFolder = r'E:\data\subject\file\subjectResult\subject26Files'
    #inputFolder = r'E:\data\subject\file\D424Token'
    #inputFolder = r'E:\data\subject\subjectResult\s25\Input'
    
    inputFolder = os.path.join(subFolder, 'selectFiles')
    folders, fnames, texts = getTextsFromFolder(inputFolder)

    print 'end of read files! length of file = %s'%(len(fnames))
    
    #建立词典，即bag-of-words,放在每个专题之下
    #middlefolder = r'E:\data\subject\file\Dict&Model'
    
    middlefolder = os.path.join(subFolder, 'Dict&Model')
    cleanDir(middlefolder) 
    dictName = 'longTest.dict'
    #dictName = 's26_WordTest.dict'
    dictionary = genDict(texts)
    dictionary.save(os.path.join(middlefolder, dictName))
    #dictionary = corpora.Dictionary.load(os.path.join(middlefolder, dictName))
    
    #建立lsi模型并计算所有文章索引
    #lsiModeName = 'shortLSI.model'
    #lsiIdxName = 'shortLSI.index'
    lsiModeName = 'wordLSI.model'
    lsiIdxName = 'wordLSI.index'
    mod, index = calLSIandIndex(dictionary, texts);
    mod.save(os.path.join(middlefolder, lsiModeName))
    index.save(os.path.join(middlefolder, lsiIdxName))
    #mod = models.LsiModel.load(os.path.join(middlefolder, lsiModeName))
    #index = similarities.MatrixSimilarity.load(os.path.join(middlefolder, lsiIdxName))
    
    #建立LDA模型并计算所有文章索引
    #LDAModeName = 'shortLDA.model'
    #LDAIdxName = 'shortLDA.index'
    #LDAModeName = 'longLDA.model'
    #LDAIdxName = 'longLDA.index'
    #mod, index = calLDAandIndex(dictionary, texts);
    #mod.save(os.path.join(middlefolder, LDAModeName))
    #index.save(os.path.join(middlefolder, LDAIdxName))
    #mod = models.LdaModel.load(os.path.join(middlefolder, LDAModeName))
    #index = similarities.MatrixSimilarity.load(os.path.join(middlefolder, LDAIdxName))
    
    #构建二维列表，每一行为文章ID+被选中的关键词
    fileRecords = []
    for each in fnames:
        eachFile = []
        fileCtts = each.split('_')
        eachFile.append(fileCtts[0])
        fileRecords.append(eachFile)
    
    #读入专题关键词表,每个关键词为一行
    fileName = 'keyWordFile.txt'
    resultFolder = os.path.join(subFolder, 'Result')
    fullName = os.path.join(resultFolder, fileName)
    wordRecordFile = open(fullName, 'w')
    
    #fname = r'E:\data\subject\wordDict\subjectKeywords\subject25.txt'
    fullName = os.path.join(subFolder, keywordFile)
    fkeywords = open(fullName, 'r')
    
    #resultFolder = r'E:\data\subject\file\subjectResult\s25\Result\Keywords'
    keywordFolder = os.path.join(subFolder, 'Keywords')
        
    for eachLine in fkeywords:
        keyword = eachLine.strip()
        keywords = keyword.split(' ')
        query_words = []
        for eachWord in keywords:
            query_words.append(eachWord)
        query_bow = dictionary.doc2bow(query_words)
    
        print 'keyword=%s, write result...'%(keyword)
        wordRecordFile.write(keyword)
    
        sort_sims = querybyMolIndex(mod, index, query_bow)
        sortCounter = 0
        #resultFolder = r'E:\data\subject\file\subjectResult\s25\Result\Keywords'
        fileName = keyword.decode('utf-8') + '_LSI_500.txt'
        fullName = os.path.join(keywordFolder, fileName)
        sortFile = open(fullName, 'w')
        for item in sort_sims:
            print "item[1]=%s" %item[1]
            if item[1] < 0.05:
                break
            filename = fnames[item[0]].decode('gbk').encode('utf-8')
            print 'LSI: index=%s, filename is %s, sims=%s'%(item[0], filename, item[1])
            sortFile.write('LSI: index=%s, filename is %s, sims=%s'%(item[0], filename, item[1]))
            sortFile.write('\n')
            
            #将挑选出的文章ID写入对应关键词的一行
            fileCtts = filename.split('_')
            wordRecordFile.write(', '+fileCtts[0])
            
            #将该关键词写入对应文章的一行
            fileRecords[item[0]].append(keyword)
            
            sortCounter += 1
            
        wordRecordFile.write('\n')
        #print 'write result end!'
        sortFile.close()
        
    fkeywords.close()
    wordRecordFile.close()
    
    #将二维列表写为文件
    fileName = 'fileRecord.txt'
    fullName = os.path.join(resultFolder, fileName)
    fileCounter = 0
    #FileRecord = open(r'E:\data\subject\file\subjectResult\s25\Result\fileRecord.txt', 'w')
    FileRecord = open(fullName, 'w')
    for eachFile in fileRecords:
        for eachWord in eachFile:
            FileRecord.write(eachWord+', ')
        if len(eachFile) > 1:
            fileCounter += 1
        FileRecord.write('\n')
    
    FileRecord.write('Total select file number is %s. \n'%(fileCounter))
    print 'FileRecord = %s'%(FileRecord)
    FileRecord.close()
    
    print 'keywords process end!'
   
#专用于为docTest生成输入文件夹列表
def getInFolders(sudFolder):     
    inFolders = []
    #folder = r'E:\data\subject\subjectResult\s1\tokenOutput'
    folder = os.path.join(sudFolder, 'tokenOutput')
    subFolders = os.listdir(folder)
    for each in subFolders:
        inputFolder = os.path.join(folder, each) 
        inFolders.append(inputFolder)          
    return inFolders;
        

#下面为测试程序
if __name__ =="__main__":
    
    print 'Start to process'
    fileCount = 0;
    
    for i in range(1,27):
        #生成专题文件夹
        sFolderName = 's'+str(i)
        #sudFolder = r'E:\data\subject\subjectResult\s1'
        sudFolder = os.path.join(r'E:\data\subject\subjectResult', sFolderName)
        
        #生成专题关键词
        #keywordsFile = 'subject1.txt'
        keywordsFile = 'subject'+str(i)+'.txt'
        
        #生成输入文件夹列表.
        inFolders = getInFolders(sudFolder)
        #从输入文件夹下的所有文件中挑出满足条件的文件,保存在selectFiles文件夹下
        fileCount += docTest(inFolders, sudFolder, keywordsFile)
        
        #为每个关键词分配文章,将selectFiles文件夹下的文章与关键字建立对应
        wordTest(sudFolder, keywordsFile)
        
        print 'Subject %d process end!'%(i)
        
        #test
        #break

    print 'End! select fileCount=%d'%fileCount
    
    
    
