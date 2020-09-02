#!/usr/bin/python3
# -*- coding:utf-8 -*-
'''
Created on 2020年9月2日
@author: yuejing
'''
from py2neo import Node,Relationship,Graph,Subgraph,NodeMatcher,RelationshipMatcher
import pandas as pd

class go_neo4j:
	def __init__(self,host='http://10.10.10.71:7474',username='neo4j',password='neo4j'):
		#连接neo4j
		self.graph = Graph(host, username=username, password=password)
		#初始化neo4j，清空数据

	#清空数据
	def wipe_data(self):
		self.graph.delete_all()
		print('neo4j数据已清空！')

	#批量创建标签
	def create_node(self,lable,datas):
		'''
		lable为标签名
		datas为标签数据，有两种格式：
			1.[节点]
			2.[节点,{节点属性}]
		'''
		try:
			node=[]
			datalists=[]
			#节点去重
			for data in datas:
				if data not in datalists:
					datalists.append(data)
			for datalist in datalists:
				#没有属性
				if isinstance(datalist,str):
					#创建节点
					node_temp=Node(lable,name=datalist)
					#聚合所有节点
					node.append(node_temp)
				#节点属性
				elif len(datalist)==2:
					#创建节点
					node_temp=Node(lable,name=datalist[0])
					#更新节点属性
					node_temp.update(datalist[1])
					#聚合所有节点
					node.append(node_temp)
				else:
					print('输入数据错误！需对应 [节点] or [节点,{属性}]')
			#创建标签
			nodes=Subgraph(node)
			self.graph.create(nodes)
			#打印创建结果
			for i in range(len(node)):
				print('创建节点： ',str(node[i]).encode('utf-8').decode('unicode_escape'))

		except Exception as e:
			print(e)

	#批量创建关系
	def create_relationship(self,a_lable,b_lable,datas):
		'''
		a_lable为主标签名
		b_lable为从标签名
		datas有两种格式：
			1.[主,从,关系]
			2.[主,从,关系,{关系属性}]
		'''
		try:
			rel=[]
			datalists=[]
			#关系去重
			for data in datas:
				if data not in datalists:
					datalists.append(data)
			#创建关系
			for datalist in datalists:
				if len(datalist)==3:
					#获取[主-从-关系]数据
					a_name=str(datalist[0])
					b_name=str(datalist[1])
					Relation=str(datalist[2])
					#match标签
					a_node=self.graph.nodes.match(a_lable,name=a_name).first()
					b_node=self.graph.nodes.match(b_lable,name=b_name).first()
					#聚合所有关系
					rel_temp=Relationship(a_node , Relation, b_node)
					rel.append(rel_temp)
				elif len(datalist)==4:
					#获取[主-从-关系-关系属性]数据
					a_name=str(datalist[0])
					b_name=str(datalist[1])
					Relation=str(datalist[2])
					properties=datalist[3]
					#match标签
					a_node=self.graph.nodes.match(a_lable,name=a_name).first()
					b_node=self.graph.nodes.match(b_lable,name=b_name).first()
					#聚合所有关系
					rel_temp=Relationship(a_node , Relation, b_node,**properties)
					rel.append(rel_temp)
				else:
					print('输入数据错误！需对应 [主,从,关系] or [主,从,关系,{关系属性}]')
			#创建关系
			rels=Subgraph(relationships=rel)
			self.graph.create(rels)
			#打印创建结果
			for i in range(len(rel)):
				print('创建关系： ',str(rel[i]).encode('utf-8').decode('unicode_escape'))

		except Exception as e:
			print(e)

	#查询CQL
	def search_cql(self,match_str):
		result=self.graph.run(match_str)
		return result

	#查询节点
	def nodeMatcher(self):
		matcher=NodeMatcher(self.graph)
		return matcher

	#查询关系
	def relationshipMatcher(self):
		matcher=RelationshipMatcher(self.graph)
		return matcher

if __name__ == "__main__":
	#读取excel数据
	df=pd.read_excel('data.xlsx')

	#dataframe转成list，若非嵌套list则先新建再更新
	sell=df[['sell','sell_sex']].values.tolist()
	for i in range(len(sell)):
		sell[i][1]={'sex': sell[i][1]}
	#dataframe转成list，若非嵌套list则先新建再更新
	buy=df[['buy','buy_sex']].values.tolist()
	for i in range(len(buy)):
		buy[i][1]={'sex': buy[i][1]}

	#关系数据
	data=df[['sell','buy','money']].values.tolist()
	#添加关系属性
	for i in range(len(data)):
		data[i]+=[{'name':'销售金额','unit':'万元'}]

	#初始化函数
	getdata=go_neo4j()
	#清空数据
	getdata.wipe_data()
	#创建标签
	getdata.create_node('buy',buy)
	getdata.create_node('sell',sell)
	#创建关系
	getdata.create_relationship('sell','buy',data)

	#查询
	res1=getdata.search_cql('MATCH p=()-[r:`20`]->() RETURN p')
	res2=list(getdata.nodeMatcher().match(name='jimmy'))
	res3=list(getdata.relationshipMatcher().match(r_type='10'))
	print(res3)

	
