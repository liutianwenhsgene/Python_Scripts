from mylib import *
import time
import datetime
from bson.objectid import ObjectId
'''
age_stage
0:

'''
def INHERE(A,L,R):
	s1=False
	s2=False
	if L=='L':
		s1=True
	else:
		if int(A)>=int(L):
			s1=True
		else:
			pass
	if R=='R':
		s2=True
	else:
		if int(A)<int(R):
			s2=True
		else:
			pass
	return s1 and s2

def test():
	print('start')
	sql = sqlbase("databases.config","<source-mysql>")
	mgo = mongobase("databases.config","<source-mongo>")
	sql2= sqlbase("databases.config","<target-mysql>")
	coll = mgo.get_collection()
	side_score={}
	data = sql2.get_data_by_query('select value,score from sidereaction_score')

	DIC=sql.get_dict_list('dic_list.txt')
	for d in data:
		side_score[d[0]]=int(d[1])

	rank_score={}
	with codecs.open('score_rank.txt',encoding='utf-8') as f:
		f.readline()
		for each in f:
			lines=each.strip().split()
			rank_score[lines[0]]={'L':lines[1],'R':lines[2]}


	Hos=[]
	section=[]
	with codecs.open('hospital_section_list.txt',encoding='utf-8') as f:
		f.readline()
		for each in f:
			lines=each.strip().split()
			Hos.append(lines[0])
			section.append(lines[1])
	approve_Map ={}
	for i in range(len(Hos)):
		#print(Hos[i]+' '+section[i])
		sql.get_approve_Map(approve_Map ,query='select A.ref_id,substr(A.submit_time,1,7),substr(A.approve_time,1,7),A.member_id,A.state \
		from dcw_base as A join da_member as B ON A.member_id = B.id \
		where  B.real_hospital="'+Hos[i]+'" and B.real_dept="'+section[i]+'" and A.state=5 and A.repeat_state!=4 ')
	
	count=0
	querys=''
	for case in coll.find({},{"caseNo":1,"medical":1,"patient":1}):
		sum=0
		rank='UNKNOWN'
		sideReaction_list={}
		if str(case['_id']) not in approve_Map.keys() or approve_Map[str(case['_id'])]['state'] in ["0","1"]:
			continue

		try:
			#chrt_l=len(case['medical']['chrt'])

			for each_chrt in case['medical']['chrt']:
				try:
					for each_sideReactionCatalogyName in each_chrt['sideReaction'].keys():
						
						if len(each_chrt['sideReaction'][each_sideReactionCatalogyName])>0:

							for each_sideReaction in each_chrt['sideReaction'][each_sideReactionCatalogyName]:
								try:
									if each_sideReaction in side_score.keys():
										sideReaction_list[each_sideReaction]=side_score[each_sideReaction]
								except:
									pass
								
				except:
					pass
		except:
			pass
		for each in sideReaction_list.keys():
			sum+=sideReaction_list[each]
		if sum==0 and len(sideReaction_list.keys())==0:
			sum=-1

		for each in rank_score.keys():
			if INHERE(sum,rank_score[each]['L'],rank_score[each]['R']):
				rank=each
				break
		submit_hospital_id=''
		submit_hospital_name=''
		submit_section_id=''
		submit_section_name=''
		case_hospital_id=''
		case_hospital_name=''
		case_section_id=''
		case_section_name=''

		try:
			submit_hospital_id=DIC['M_H'][approve_Map[str(case['_id'])]['member_id']]
			submit_hospital_name=DIC['hos'][submit_hospital_id]
		except:
			pass
		try:
			submit_section_id=DIC['M_D'][approve_Map[str(case['_id'])]['member_id']]
			submit_section_name=DIC['dept'][submit_section_id]
		except:
			pass
		try:
			case_hospital_id=case['patient']['basic']['hospital']
			case_hospital_name=DIC['hos'][case_hospital_id]
		except:
			pass
		try:
			case_section_id=case['patient']['basic']['hospitalSection']
			case_section_name=DIC['dept'][case_section_id]
		except:
			pass

		#print(submit_hospital_name+':'+submit_section_name+':'+case_hospital_name+':'+case_section_name)
		
		H_D='`submit_hospital_id`,`submit_hospital_name`,`submit_section_id`,`submit_section_name`,`case_hospital_id`,`case_hospital_name`,`case_section_id`,`case_section_name`'
		H_D_data='"'+submit_hospital_id+'"'+','+'"'+submit_hospital_name+'"'+','+'"'+submit_section_id+'"'+','+'"'+submit_section_name+'"'+','+'"'+case_hospital_id+'"'+','+'"'+case_hospital_name+'"'+','+'"'+case_section_id+'"'+','+'"'+case_section_name+'"'
		

		query='REPLACE INTO dcw_sidereaction_score (`dcw_id`,`sidereaction_score`,`rank`'+','+H_D+') VALUES('+'"'+case['caseNo']+'"'+','+str(sum)+','+'"'+rank+'" '+','+H_D_data+');'
		querys=querys+query+'\n'
		
		count+=1
		if count%1000==0:
			print(count)
			try:
				sql2.cur.execute(querys)
				sql2.conn.commit()
			except Exception as e:
				print(e)
				print('插入出问题')
				sql2.conn.rollback()
			querys=''
	print(count)
	try:
		sql2.cur.execute(querys)
		sql2.conn.commit()
	except Exception as e:
		print(e)
		print('插入出问题')
		sql2.conn.rollback()
def test1():
	print('start')
	mgo=mongobase()
	coll = mgo.get_collection()
	data = coll.find_one({},{"caseNo":1,"patient":1,"medical":1,"inspection":1})
	pprint.pprint(str(data['_id'])) 

if __name__=='__main__':
	test()