
import PyPDF2
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
import os
import re
import itertools
#source: https://stackoverflow.com/questions/26494211/extracting-text-from-a-pdf-file-using-pdfminer-in-python
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import StringIO
import logging
from datetime import datetime
import csv

class SentimentScore():
    
    def __init__(self, workingPath):
        self.workingPath = workingPath   
        self.excludedFolder = ['Archive', 'Thumbs.db', 'Earnings template - Shortcut.lnk',
                               '1_Overall Summary', 'Testing']
        self.sectorList = ['Communications', 'Consumer Discretionary', 'Consumer Staples',
                           'Energy', 'Healthcare', 'Industrials',
                           'Materials', 'REITS', 'Tech', 'Utilities']
        'userLog_{}.txt'.format(datetime.now().strftime("%Y-%m-%d"))
        self.excelFile = '../Data/Interim/ProcessedTickerFinal_{}.xlsx'.format(datetime.now().strftime("%Y-%m-%d"))
        self.failedFileCsv = '../Data/Log/FailedFileList_{}.csv'.format(datetime.now().strftime("%Y-%m-%d"))
        self.cutSentenceFile = "../Data/External/CutSentence.xlsx"
        self.fileList = []
        self.wpList = []
        self.pdfTextList = []
        self.periodList = []
        self.yearList = []
        self.stockNameList = []
        self.extractFileFailedList = []
        self.processFileFailedList = []
        self.successFileList = []
        self.successFinalFileList = []
        self.reviewList = []
        self.assessmentList = []
        self.detailsList = []
        self.announcementDateList = []
        self.sentimentScore = []
        self.sentimentScore_Magnify = []
        self.sentimentScore_Override = []
        self.processedTextList = []
        self.failedIndexList = []
        
        #log file
        logFilePath = '../Data/Log/ProcessingRecord'
        logFileName = datetime.now().strftime('{}__%Y-%m-%d.log'.format(logFilePath))
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)    
        logging.basicConfig(filename = logFileName, filemode = 'a', 
                            level = logging.DEBUG, format = '%(levelname)s:%(message)s')

        #cut sentence dict
        df = pd.read_excel(self.cutSentenceFile)
        self.cutSentenceDict = df.set_index('Original').to_dict()['Replacement']    

    def extractAllFiles(self, wp):
        for file in os.listdir(wp):
            if os.path.isfile(wp + '/' + file):
                if '.pdf' in file:
                    self.fileList.append(file)
                    self.wpList.append(wp)
            elif os.path.isdir(wp + '/' + file):
                if file not in self.excludedFolder:
                    self.extractAllFiles(wp + '/' + file)
                    
    #read and extract text from one pdf file - 2nd attempt using pypdf2 if pdfminer fails
    def convertPdfToTxt_m2(self, fullPath):
        pdf_file = open(fullPath, 'rb')
        read_pdf = PyPDF2.PdfFileReader(pdf_file)
        page = read_pdf.getPage(0)
        page_content = page.extractText()
        return page_content
                    
    #read and extract text from one pdf file using pdfminer
    def convertPdfToTxt(self, path):
        rsrcmgr = PDFResourceManager()
        retstr = StringIO()
        #codec = 'utf-8'
        laparams = LAParams()
        device = TextConverter(rsrcmgr, retstr, laparams=laparams)
        fp = open(path, 'rb')
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        password = ""
        maxpages = 0
        caching = True
        pagenos=set()
        for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password,caching=caching, check_extractable=True):
            interpreter.process_page(page)
        text = retstr.getvalue()
        fp.close()
        device.close()
        retstr.close()
        return text
      
    def extractAllPDF(self):
        for no, file in enumerate(zip(self.wpList, self.fileList)):

            try:
                #extract pdf content
                infoMsg = 'Extracting text from pdf file - pdfminer: %s'%file[0] + '/' + file[1]
                print(infoMsg)
                logging.info(infoMsg)
                pdf_content = self.convertPdfToTxt(file[0] + '/' + file[1])
                
                infoMsg = 'Extracting metadata from filenames like stockName, period and year\n'
                print(infoMsg)
                logging.info(infoMsg)
                stockName = file[1].split(' ')[0] + '_' + file[1].split(' ')[1]
                #not necessary manage to get period
                if re.search('(.{2} )\d{4}.*', file[1]):
                    period = re.search('(.{2} )\d{4}.*', file[1]).group(1)
                else:
                    period = ''
                year = re.search('(\d{4}) (Earnings|Sales|Revenue).*', file[1]).group(1)
                self.periodList.append(period)
                self.yearList.append(year)
                self.stockNameList.append(stockName)
                self.pdfTextList.append(pdf_content)
                self.successFileList.append(file[0] + '/' + file[1])

            except Exception as e:
                print(e)
                errorMsg = 'Failed to extract text/metadata from file - pdfminer: %s'%file[0] + '/' + file[1] 
                print(errorMsg)
                logging.error(e)
                logging.error(errorMsg)
                self.extractFileFailedList.append(file[0] + '/' + file[1])
        
        #keep records of failed files in first attempt of extraction
        self.firstTimeFailedList = self.extractFileFailedList
        
        for failedFile in self.extractFileFailedList:
            
            try:
                infoMsg = '2nd attempt of extracting text from pdf file - pypdf2: %s'%failedFile
                print(infoMsg)
                logging.info(infoMsg)
                pdf_content = self.convertPdfToTxt_m2(failedFile)  
                
                #stock name
                infoMsg = 'Extracting metadata from filenames like stockName, period and year \n'
                print(infoMsg)
                logging.info(infoMsg)
                stockName = failedFile.split('/')[-1].split(' ')[0] + '_' + failedFile.split('/')[-1].split(' ')[1]
                if re.search('(.{2} )\d{4}.*',  failedFile.split('/')[-1]):
                    period = re.search('(.{2} )\d{4}.*',  failedFile.split('/')[-1]).group(1)
                else:
                    period = ''
                year = re.search('(\d{4}) (Earnings|Sales|Revenue).*', failedFile.split('/')[-1]).group(1)
                self.periodList.append(period)
                self.yearList.append(year)
                self.stockNameList.append(stockName)
                self.successFileList.append(failedFile)
                self.pdfTextList.append(pdf_content)
                #remove file names from failed list
                self.extractFileFailedList = [i for i in self.extractFileFailedList if i!=failedFile]
                    
            except Exception as e:
                print(e)
                errorMsg = 'Failed to extract text/metadata from file - pypdf2: %s'%failedFile + '\n'
                print(errorMsg)
                logging.error(e)
                logging.error(errorMsg)


    def cutSentence(self, text):
        #cut sentence
        if re.search('|'.join(self.cutSentenceDict.keys()), text):
            matchList = re.findall('|'.join(self.cutSentenceDict.keys()), text)
            for match in matchList:
                text = re.sub(match, self.cutSentenceDict[match], text)     
        return text

    #between -1 (most extreme negative) and +1 (most extreme positive)
    def giveSentimentScore(self, text):
        analyzer = SentimentIntensityAnalyzer()
        #cut sentence
        text = self.cutSentence(text)
        sentimentscore = analyzer.polarity_scores(text)
        score = sentimentscore['compound']
        return score

    def categorizeScore(self, score):
        # positive sentiment: compound score >= 0.05
        # neutral sentiment: (compound score > -0.05) and (compound score < 0.05)
        # negative sentiment: compound score <= -0.05
        positivefunc = lambda x:x > 0.05
        neutralfunc = lambda x:x>-0.05 and x <0.05
        negativefunc = lambda x:x<=-0.05
        category_func = {'Positive':positivefunc, 'Neutral':neutralfunc, 'Negative':negativefunc}
        return self.runAllFunc(category_func, score)

    def runAllFunc(self, func_dict, value):
        for func in func_dict:
            if func_dict[func](value):
                return func
            
    def magnifyScore(self, text, overriding = False):
        splitPoint = 'Share price reaction:'
        firstText = text.split(splitPoint)[0]
        # secondText = re.sub('\-','minus ', re.sub('\+', 'plus ', text.split(splitPoint)[1]))
        #if no share price reaction in section of more details
        if len(text.split(splitPoint)) == 1:
            secondText = re.sub('\-','minus ', re.sub('\+', 'plus ', text.split(splitPoint)[0]))
        else:
            secondText = re.sub('\-','minus ', re.sub('\+', 'plus ', text.split(splitPoint)[1]))
        maxNum = 5
        magnifier = 3

        weightFirstText = 1/(magnifier + 1)
        weightSecondText = magnifier/(magnifier + 1)
        #use number in percent as weightage
        if re.search('-*(\d+(\.\d+)*)%', secondText):
            numberPercent = float(re.search('(\d+(\.\d+)*)%', secondText).group(1))
            
            #try to identify key phrase preceding numberPercent
            PositivePhraseList = ["rose", "rose by", "soared", "soared as much as"]
            NegativePhraseList = ["declined", "declined by", "slumped", "slumped by", "fell", "fell by"]
            #check if there is positive/negative phrase preceding numberPercent
            if re.search("(" + "|".join(PositivePhraseList + NegativePhraseList) + ")\s%s"%numberPercent, secondText):
                secondText = re.search("(" + "|".join(PositivePhraseList + NegativePhraseList) + ")\s%s"%numberPercent, secondText).group(1)
                keyPhrase = secondText
            else:
                keyPhrase = ''
                
            #if number is more than maxNum
            if numberPercent >= maxNum:
                #if overriding is selected
                if overriding == True:
                    #print('Sentiment (second part text): %s'%self.giveSentimentScore(secondText))
                    return self.giveSentimentScore(secondText)
                else:
                    numberWeight = 1
                    #if no keyPhrase identified
                    if keyPhrase == "":
                        secondText = re.search('(.*?\d+(\.\d+)*%(\s\w+){0,2})', secondText).group(1)
                    if self.giveSentimentScore(secondText) == 0:
                        return weightFirstText*self.giveSentimentScore(firstText)
                    else:
                        return weightFirstText*self.giveSentimentScore(firstText) + weightSecondText*numberWeight*(self.giveSentimentScore(secondText)/abs(self.giveSentimentScore(secondText)))
            else:
                numberWeight = numberPercent/maxNum
                #print('Sentiment (second part text): %s'%self.giveSentimentScore(secondText))
                #print('Number of weight: %s'%numberWeight)
                return weightFirstText*self.giveSentimentScore(firstText) + weightSecondText*numberWeight*self.giveSentimentScore(secondText)
        else:
            numberWeight = 1
            #print('Sentiment (second part text): %s'%self.giveSentimentScore(secondText))
            #print('Number of weight: %s'%numberWeight)
            return weightFirstText*self.giveSentimentScore(firstText) + weightSecondText*numberWeight*self.giveSentimentScore(secondText)
            
    #group content - pdfminer based on results review, assessment, more details 
    def groupText(self, content):
        #topicList = ['RESULTS REVIEW:', 'ASSESSMENT:', 'MORE DETAILS:'] 
        topicList = ['RAHSIA', 'ASSESSMENT:', 'MORE DETAILS:']
        contentList = content.split('\n')
        if topicList[0] in content:
            review = ' '.join([i.strip() for i in contentList[contentList.index(topicList[0]) + 1:contentList.index(topicList[1])]
 if i not in  ['', 'RESULTS REVIEW:']])
        elif 'RESULTS REVIEW:' in content:
            review = ' '.join([i.strip() for i in contentList[contentList.index('RESULTS REVIEW:') + 1:contentList.index(topicList[1])]
                          if i not in  ['', 'RESULTS REVIEW:']])  
        assessment = ' '.join([i.strip() for i in contentList[contentList.index(topicList[1]) + 1:contentList.index(topicList[2]) - 1]
                          if i not in ['', ':', 'comments:']])
        details = ' '.join([i.strip() for i in contentList[contentList.index(topicList[2]):-2]
                           if i not in ['',':']])
        if 'Announcement date:' in contentList[-1] and re.search('Announcement date: (.+?\d{4})', contentList[-1]):
            announcementDate = re.search('Announcement date: (.+?\d{4})', contentList[-1]).group(1)
        else:
            announcementDate = ''
        return review, assessment, details, announcementDate
      
    #group content - pypdf2 based on results review, assessment, more details
    def groupText2(self, content):
        topicList = ['RESULTS REVIEW:', 'ASSESSMENT:', 'MORE DETAILS:']

        contentList = content.split('\n')
        review = ' '.join([i.strip() for i in contentList[contentList.index(topicList[0]) + 1:contentList.index(topicList[1])]
                          if i!= ''])
        assessment = ' '.join([i.strip() for i in contentList[contentList.index(topicList[1]) + 1:contentList.index(topicList[2]) - 1]
                          if i not in ['', ':', 'comments:']])
        contentList2 = [i for i in contentList if i != '']
        details = ' '.join([re.sub('Ł', '', i.strip()) for i in contentList2[contentList2.index(topicList[2]) + 
                                          1:] if i not in [':']])
        if 'Announcement date:' in contentList[-1] and re.search('Announcement date: (.+?\d{4})', contentList[-1]):
            announcementDate = re.search('Announcement date: (.+?\d{4})', groupText(pdf2)[-1]).group(1)
        else:
            announcementDate = ''
        return review, assessment, details, announcementDate
                
    def processText(self):      
        for no, text in enumerate(self.pdfTextList):
            try:
                print('Grouping text to review, assessment and details from pdf file: %s'%self.successFileList[no])
                if self.successFileList[no] not in self.firstTimeFailedList:
                    reviewText, assessmentText, detailsText, announcementDateText = self.groupText(text)
                else:
                    reviewText, assessmentText, detailsText, announcementDateText = self.groupText2(text)

                print('Assigning sentiment scores to pdf file: %s \n'%self.successFileList[no])
                for num, processedText in enumerate([reviewText, assessmentText, detailsText]):
                    self.sentimentScore.append(self.giveSentimentScore(processedText))
                    if num == 0:
                        self.sentimentScore_Magnify.append(self.magnifyScore(processedText))
                        #self.sentimentScore_Override.append(self.magnifyScore(processedText, overriding = True))
                    else:
                        self.sentimentScore_Magnify.append(self.giveSentimentScore(processedText))
                        #self.sentimentScore_Override.append(self.giveSentimentScore(processedText))
                    
                    #store information after everything is ok
                    self.processedTextList.append(processedText)
                    
                self.reviewList.append(reviewText)
                self.assessmentList.append(assessmentText)
                self.detailsList.append(detailsText)
                self.announcementDateList.append(announcementDateText)
                self.successFinalFileList.append(self.successFileList[no])
                
            except Exception as e:
                print(e)
                errorMsg = 'Failed to group text or assign scores for file: %s \n'%self.successFileList[no]
                print('Failed to group text or assign scores for file: %s \n'%self.successFileList[no])
                logging.error(e)
                logging.error(errorMsg)
                self.processFileFailedList.append(self.successFileList[no])
                self.failedIndexList.append(no)
                                
        #remove stock name, period, year according to location
        self.stockNameList = [i for no, i in enumerate(self.stockNameList) if no not in self.failedIndexList]
        self.periodList = [i for no, i in enumerate(self.periodList) if no not in self.failedIndexList]
        self.yearList = [i for no, i in enumerate(self.yearList) if no not in self.failedIndexList]
        #for experiment
        self.pdfTextList= [i for no, i in enumerate(self.pdfTextList) if no not in self.failedIndexList]
                
    def prepareDict(self):
        self.finalDict = {'Period': list(itertools.chain(*[[i]*3 for i in self.periodList])),
            'Year': list(itertools.chain(*[[i]*3 for i in self.yearList])),
            'AnnouncementDate':list(itertools.chain(*[[i]*3 for i in self.announcementDateList])),
            'WorkingPath': list(itertools.chain(*[[i]*3 for i in self.successFinalFileList])),
            'Content': self.processedTextList,
            'ContentType': ['Review', 'Assessment', 'Other details']*len(self.stockNameList),
            'StockName': list(itertools.chain(*[[i]*3 for i in self.stockNameList])),
            # 'SentimentScore': self.sentimentScore,
            'SentimentScore_Magnify':self.sentimentScore_Magnify}
        
        return self.finalDict
    
    def prepareDF(self):
        self.df = pd.DataFrame(self.finalDict)

        #map period series
        periodDict = {'Q1':'1Q', 'H1':'1H', 'H2':'2H','Q3':'3Q', 'Q2':'2Q', 'Q4':'4Q'}
        monthDict = {'':'', 1:'1Q', 2:'1Q', 3:'1Q', 4:'2Q', 5:'2Q', 6:'2Q',
                     7:'3Q', 8:'3Q', 9:'3Q', 10:'4Q', 11:'4Q', 12:'4Q'}
        self.df['Period'] = self.df['Period'].map(lambda x: periodDict[x.strip()]
                                       if x.strip() in periodDict else x.strip())
        self.df.sort_values(['Year', 'Period'], inplace = True)
        
        #sector mapping, regex from working path
        self.df['Sector'] = self.df['WorkingPath'].map(lambda x:re.search('(%s)'%'|'.join(self.sectorList), x).group(1)
                                 if re.search('|'.join(self.sectorList), x) else '')
        colList = [i for i in self.df.columns if i not in ['WorkingPath']]
        #self.df = self.df[colList]
        
        #ad hoc manipulation
        self.df = (self.df.filter(colList)
                          .assign(FullStockName = lambda x:x['StockName'])
                          .assign(StockNameTemp = lambda x:x['FullStockName'].str.split('_'))
                           .assign(StockName = lambda x:x['StockNameTemp'].map(lambda y:y[0])))
        del self.df['StockNameTemp']
        #return self.df
    
    def calculateScore(self):
        print('Creating Dict to map entity to announcementDate')
        #create dict for date mapping
        DateDict = self.df[['FullStockName', 'Period', 'Year', 'AnnouncementDate']]\
                    .set_index(['FullStockName', 'Period', 'Year'])\
                    .to_dict()['AnnouncementDate']
        
        print('Calculating Sentiment Score')
        self.dfFinal = (self.df.filter(['FullStockName', 'Sector', 'Period', 'Year', 'ContentType', 'SentimentScore_Magnify'])
                                .pivot_table(values = 'SentimentScore_Magnify', 
                                             index = ['FullStockName', 'Sector', 'Period', 'Year'], 
                                             columns = 'ContentType')
                                .reset_index()
                                .assign(OverallScore = lambda x:0.4*x['Assessment'] + 
                                                0.4*x['Review'] + 0.2*x['Other details'])
                                .assign(AnnouncementDate = lambda x:x.apply(lambda y:DateDict[(y['FullStockName'], y['Period'], y['Year'])], axis = 1)))
    
    def finalProcessing(self):
        #ad hoc manipulation
        sectorDict = {'Communications':'Communication Services', 'Energy':'Energy', 'REITS':'Real Estate',
                    'Consumer Discretionary':'Consumer Discretionary', 'Tech':'Information Technology',
                    'Industrials':'Industrials', 'Consumer Staples':'Consumer Staples',
                    'Industrials':'Industrials', 'Healthcare':'Health Care', 'Materials':'Materials',
                    'Utilities':'Utilities'}
        self.dfFinal['Sector'] = self.dfFinal['Sector'].map(sectorDict)

        self.dfFinal['Final CY Period'] = self.dfFinal['Year'] + self.dfFinal['Period'].map(lambda x:''.join(sorted(x, reverse = True)))\
                                                        .map({'H2':'Q4','H1':'Q2', 'Q1':'Q1', 'Q2':'Q2',
                                                            'Q3':'Q3', 'Q4':'Q4'})

        self.dfFinal['Ticker'] = self.dfFinal['FullStockName'].map(lambda x:re.sub('_', ' ', x) + ' Equity')

        self.dfFinal['Announcement Date'] = self.dfFinal['AnnouncementDate'].map(lambda x:x.replace(' July ', '/7/').replace('-May-', '/05/')
                                            .replace('-Oct-', '/10/').replace(' May ', '/05/'))

        self.dfFinal.rename(columns = {'OverallScore':'Stock SentimentScore'}, inplace = True)

    def writeResult(self):
        print('Saving failed file list')
        #save failed file list
        with open(self.failedFileCsv, 'w') as writeFile:
            writer = csv.writer(writeFile)
            #extraction failure
            writer.writerow(['Extraction Failed'])
            for file in self.extractFileFailedList:
                writer.writerow([file])
            #process failure
            writer.writerow(['Processing Failed'])
            for file in self.processFileFailedList:
                writer.writerow([file])
        
        #save table from calculateScore
        #save processed table
        print('Saving final processed table')
        colList = ['Ticker', 'Sector', 'Announcement Date', 'Stock SentimentScore']
        self.dfFinal[colList].to_excel(self.excelFile, index = False, encoding = 'utf-8')
        return self.dfFinal
        
    def mainFlow(self):
        self.extractAllFiles(self.workingPath)
        self.extractAllPDF()
        self.processText()
        self.prepareDict()
        self.prepareDF()
        self.calculateScore()
        self.finalProcessing()
        resultDF = self.writeResult()
        
        return resultDF
