import pymongo 
import pprint
from  openpyxl.workbook  import  Workbook  
#万恶的ExcelWriter，妹的封装好了不早说，封装了很强大的excel写的功能   
from  openpyxl.writer.excel  import  ExcelWriter  
#一个eggache的数字转为列字母的方法   
from  openpyxl.cell  import  get_column_letter  
import pymysql
import time
import sys
import os
import codecs
from itertools import islice
class Record:
	info={}
	info['dcw_id']=''
	info['member_id']=''
	info['member_name']=''
	info['submit_hospital_id']=''
	info['submit_hospital_name']=''
	info['submit_hospital_section_id']=''
	info['submit_hospital_section_name']=''
	info['case_hospital_id']=''
	info['case_hospital_name']=''
	info['case_section_id']=''
	info['case_section_name']=''
	info['admission_date']=''
	info['discharge_date']=''
	info['first_admission_date']=''
	info['reside_address']=''
	info['gender']=''
	info['doctor_name']='' 
	info['admission_month']='' 
	info['first_admission_month']=''
	info['survival_period'] = ''
	info['submit_month']='' 
	info['audit_month']=''
	info['age_stage'] =''
	info['double_primary']='' 
	info['disease_name'] =''
	info['disease_phase'] =''

	def set_info(self,key,value):
		self.info[key]=value
	def __init__(self):
		pass


class mongobase:
	host = '192.168.10.162'
	port = 27017
	dbname = 'casedb'
	collection = 'cases'
	client = None
	db = None
	username=''
	password=''

	def __init__(self,filename,sign):
		info=get_config(filename,sign)
		self.host =  info['host']
		self.port = int(info['port'])
		self.collection = info['collection']
		self.dbname = info['dbname']
		if 'username' in info:
			self.username = info['username']
		if 'password' in info:
			self.password = info['password']
		print(info)
		try:
			
			self.client = pymongo.MongoClient(self.host,self.port)
			self.db = self.client[self.dbname]
			if 'username' in info and 'password' in info:
				self.db.authenticate(self.username,self.password)
		except Exception as e:
			print('连不上mongo')
			print(e)
			raise e


	def get_db(self):
		return self.db

	def get_collection(self,collection_name=collection):
		try:
			self.coll = self.db[collection_name]
			return self.coll
		except Exception as e:
			print("mongo连不上集合")
			print(e)

class sqlbase:
	'''
	host = ''
	username = ''
	password = ''
	dbname =''
	port = 0
	charsettype = ''
	sign=''
	conn =  None
	cur = None 
	'''
	def __init__(self,filename,sign):
		info=get_config(filename,sign)
		self.host =  info['host']
		self.port = int(info['port'])
		self.dbname = info['dbname']
		self.charsettype = info['charsettype']
		self.username = info['username']
		self.password = info['password']
		self.sign=sign
		try:
			print(info)
			self.conn = pymysql.connect(host=self.host,user=self.username,passwd=self.password,db=self.dbname,port=self.port,charset=self.charsettype)
			self.cur=self.conn.cursor()
		except Exception as e:
			print('无法初始化sql连接')
			raise e


	def __del__(self):
		try:
			self.conn.close()
			self.cur.close()
		except Exception as e:  
			print('sql断开连接失败')
			print(e)

	def get_sql_Collection(self):
		return cur

	def get_data_by_query(self,query):
		rownum=0
		colnum=0
		data= None
		try:
			self.cur.execute(query)
			data = self.cur.fetchall()
			rownum = len(data)
			colnum = len(data[0])
		except Exception as e:  
			print('获取数据失败')
			print(e)
		finally:
			return data

	def get_approve_Map(self,A,query='select code,submit_time,approve_time,member_id from dcw_base where state=5 and (year(approve_time)!=2016 or approve_time is null)' ):
		#获取合法的dcw的编号和基本信息，state=5，repeat！=4
		try:
			self.cur.execute(query)
			for d in self.cur.fetchall():
				A[str(d[0])]={}
				if d[1] != None :
					A[str(d[0])]['submit_time'] = d[1]
				else:
					A[str(d[0])]['submit_time'] = ''
				if d[2] != None :
					A[str(d[0])]['approve_time'] = d[2]
				else:
					A[str(d[0])]['approve_time'] = ''
				if d[3] != None :
					A[str(d[0])]['member_id'] = d[3]
				else:
					A[str(d[0])]['member_id'] = ''
				if d[4] != None :
					A[str(d[0])]['state'] = d[4]
				else:
					A[str(d[0])]['state'] = ''
		except Exception as e:  
			print('获取合适的列表失败')
			print(e)
		finally:	
			return A

	def get_dict(self,q):
		dic={}
		try:
			self.cur.execute(q)
			data=self.cur.fetchall()
			for d in data:
				dic[str(d[0])]=str(d[1])
		except Exception as e:  
			print('获取合适的sql字典失败'+' '+q)
			print(e)
		finally:		
			return dic

	def get_dict_list(self,d_f):#获取字典list，d-f文件的结构{字典名+ ：+（select key,value）}
		dic_map = {}
		DIC={}
		try:
			with codecs.open(d_f,'r',encoding='utf-8') as f:
				f.readline()
				for each in f:
					l = each.strip().split('\t')
					dic_map[l[0]]=l[1]
					#print(l[1])
			for dic_name in dic_map.keys():
				print(dic_name)
				DIC[dic_name]=self.get_dict(dic_map[dic_name])
				print(len(DIC[dic_name]))
		except Exception as e:
			print('读字典失败line-151')
			print(e)
		finally:
			return DIC

	def get_a_record_sql(self,e):
		
		col=''
		col_list=[]
		for each in e.info.keys():
			col=col+each+','
			col_list.append(each)
		col=col[:-1]
		#print(col)
		valuestr=''
		for each in col_list:
			valuestr=valuestr + '"'+e.info[each]+'"'+','
		valuestr=valuestr[:-1]
		return 'REPLACE INTO `dcw_summary`('+col+') values ('+valuestr+');'



def get_list_by_file(fn):
	A=[]
	try:
		with codecs.open(fn,encoding="utf-8") as f:
			f.readline()
			for each in f:
				E = each.strip().split()
				A.append(E[0])
	except Exception as e:
		print('读取文件列表错误')
		raise e
	finally:
		return A

def get_map_by_file(fn):
	A={}
	try:
		with codecs.open(fn,encoding="utf-8") as f:
			f.readline()
			for each in f:
				E = each.strip().split()
				#print(E)
				A[str(E[0])]={}
				A[str(E[0])]['base']=str(E[1])
				A[str(E[0])]['NodeStr']=str(E[2])
				A[str(E[0])]['dic']=str(E[3])

	except Exception as e:
		print('读取文件字典错误')
		raise e
	finally:
		return A

def num2word(dic,pros):
	word=''
	try:
		for each in pros:
			word += dic[each]+' '
	except Exception as e:  
		print('获取数据失败nums2word'+pros)
		print(e)
	finally:
		return word

def num2lastword(dic,pros):
	word=''
	try:
		for each in pros:
			word = dic[each]
	except Exception as e:  
		print('获取数据失败nums2lastword'+pros)
		print(e)
	finally:
		return word

def get_tree_node(Root,NodeStr):
	Nodelist = NodeStr.strip().split('.')
	leaf = Root
	for n in Nodelist:
		leaf = leaf[n]
	return leaf
def get_dic_value(Root,NodeStr,DIC):
	Nodelist = NodeStr.strip().split('.')
	leaf = Root
	for n in Nodelist:
		leaf = DIC[n][leaf]
	return leaf
def get_ture_str(colname,thecase,approve_list,DIC,titleMap):
	truestr=''
	try:
		if titleMap[colname]['base'] =='mongo':
			node = get_tree_node(thecase,titleMap[colname]['NodeStr'])
			if titleMap[colname]['dic'] == 'null':
				truestr = node
			else:
				if type(node)!=type([]):
					truestr = DIC[titleMap[colname]['dic']][node]
				else:
					truestr = num2lastword(DIC[titleMap[colname]['dic']],node)
		elif titleMap[colname]['base'] =='mysql':
			temp= approve_list[str(thecase['_id'])][titleMap[colname]['NodeStr']]
			if titleMap[colname]['dic'] == 'null':
				truestr = temp
			else:
				truestr = get_dic_value(temp,titleMap[colname]['dic'],DIC)
		else:
			truestr=''

	except Exception as e:
		print(获取真实数据失败)
		print(e) 
	finally:
		if len(truestr)==0:
			trustr=''
		return truestr
def get_config(filename,sign):
	info={}
	with codecs.open(filename,'r',encoding='utf-8') as f:
		symbol=0
		for each in f:
			#print(each.encode("utf-8"))
			if symbol==0 and each.strip().find(sign)>=0:
				symbol=1
			elif symbol==1 and each.strip().find(sign)<0:
				lines=each.strip().split('=')
				info[str(lines[0])]=str(lines[1])
			elif symbol==1 and each.strip().find(sign)>=0:
				break
	return info


if __name__=='__main__':


	sql = sqlbase("databases.config","<source-mysql>")
	mgo = mongobase("databases.config","<source-mongo>")
	sql2= sqlbase("databases.config","<target-mysql>")
	DIC=sql.get_dict_list('dic_list.txt')
	print(DIC['dept'])
