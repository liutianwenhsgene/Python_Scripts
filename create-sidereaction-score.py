from mylib import *
import time
import datetime
from bson.objectid import ObjectId

def test():
	print('start')
	sql = sqlbase("databases.config","<source-mysql>")
	mgo = mongobase("databases.config","<source-mongo>")
	sql2= sqlbase("databases.config","<target-mysql>")
	
	side_score={}
	with open("sr_score_list.txt") as f:
		for each in f:
			lines=each.strip().split('\t')
			side_score[lines[0]]=lines[1]

	id_score={}
	data=sql.get_data_by_query('SELECT id,label,value FROM sys_dict WHERE type="sideReaction"')
	for d in data:
		if d[1] in side_score.keys():
			id_score[d[0]]='('+'"'+d[0]+'"'+','+'"'+d[1]+'"'+','+'"'+d[2]+'"'+','+side_score[d[1]]+')'
		else:
			id_score[d[0]]='('+'"'+d[0]+'"'+','+'"'+d[1]+'"'+','+'"'+d[2]+'"'+','+'0'+')'

	for each in id_score.keys():
		try:
			sql2.cur.execute('REPLACE INTO `sidereaction_score`(`sideReaction_id`,`label`,`value`,`score`) values '+id_score[each])
			sql2.conn.commit()
		except Exception as e:
			print(e)
			print('插入出问题')
			sql2.conn.rollback()
	'''
	textlines=[]
	with codecs.open('databases.config','r',encoding='utf-8') as f:
		for each in f:
			textlines.append(each)
	textlines=textlines[:-1]

	textlines.append("<max-ref>$"+max_ref_id+"$<max-ref min=5689e2820cf272bb8b4615ab>")
	with codecs.open('databases.config','w',encoding='utf-8') as f:
		for each in textlines:
			f.write(each)'''


def test1():
	print('start')
	mgo=mongobase()
	coll = mgo.get_collection()
	data = coll.find_one({},{"caseNo":1,"patient":1,"medical":1,"inspection":1})
	pprint.pprint(str(data['_id'])) 

if __name__=='__main__':
	test()