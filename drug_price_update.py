from mylib import *
import time
import datetime


class drug:
	id=''
	price_L=0
	price_R=0
	Price_M=0
	price_unit=''


def handle_Price(s):
	s1=s.replace('￥','')
	s1=s1.replace('~',',')
	s1=s1.replace('/',',')
	lines=s1.split(',')
	L=0
	R=0
	M=0
	T=1
	unit=''
	if len(lines)==1:
		unit = 'EMPTY'
	elif len(lines)==2:
		L=R=M=format_num(lines[0])
		T,unit=get_num_unit(lines[1])
		L=R=M=L/T
	elif len(lines)==3:
		L=format_num(lines[0])
		R=format_num(lines[1])
		T,unit=get_num_unit(lines[2])

		L=L/T
		R=R/T
		M=(L+R)/2
	return L,R,M,unit

def format_num(s):
	x=0
	if s.find('万')>=0:
		x=int(s.replace('万',''))*10000
	elif s.find('千')>=0:
		x=int(s.replace('千',''))*1000
	else:
		x=int(s)
	return x
def get_num_unit(s):
	t=1
	unit='DAY'
	if s.find('月')>=0:
		unit='DAY'
		s1=s.replace('月','')
		try:
			t=int(s1)
		except:
			pass
		t=t*30
	elif s.find('mg')>=0:
		unit='MG'
		s1=s.replace('mg','')
		try:
			t=int(s1)
		except:
			pass
	elif s.find('疗程')>=0:
		unit='COURSE'
		s1=s.replace('疗程','')
		try:
			t=int(s1)
		except:
			pass
	elif s.find('天')>=0:
		unit='DAY'
		s1=s.replace('天','')
		try:
			t=int(s1)
		except:
			pass
	elif s.find('周')>=0:
		unit='DAY'
		s1=s.replace('周','')
		try:
			t=int(s1)
		except:
			pass
		t=t*7

	return t,unit
	

def test():
	print('start')
	sql = sqlbase("databases.config","<source-mysql>")
	sql2= sqlbase("databases.config","<target-mysql>")
	data=sql.get_data_by_query('select id , price FROM kn_drug_info')
	
	DRUG={}
	querys=[]
	for d in data:
		L,R,M,unit=handle_Price(str(d[1]))
		x='REPLACE INTO drug_price (`id`,`min_price`,`max_price`,`average_price`,`unit`) VALUES('+'"'+str(d[0]).strip()+'"'+','+str(L)+','+str(R)+','+str(M)+','+'"'+unit+'"'+')'
		#print (x)
		querys.append(x)
		try:
			sql2.cur.execute(x)
			sql2.conn.commit()
		except Exception as e:
			print(e)
			print('插入出问题')
			sql2.conn.rollback()
	with open('drug_price.sql','w',encoding='utf-8') as f:
		for each in querys:
			f.write(each)
			f.write(';\n')

	print(len(querys))

def test1():
	print(format_num('2000'))
	
if __name__=='__main__':
	test()