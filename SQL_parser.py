import sys
import sqlparse
import csv
import os.path
from copy import deepcopy
import itertools
argument=sys.argv[1]
if(argument[-1]==";"):
	argument=argument[:-1]    #modified argument contains the string without semicolon
aggregates=["sum","avg","max","min","distinct"]
relational_operators=['<=','>=','=','<','>']

def read_metadata(file):
	if not os.path.isfile(file):
		print("The file with name %s does not exist" %(file))
		exit()
	all_tables_info=[]
	fh = open(file)
	each_table_info=[]
	dict_table={}
	for line in fh:
		line=line.rstrip('\n')
		if(line=="<begin_table>"):
			each_table_info=[]
		elif(line=="<end_table>"):
			all_tables_info.append(each_table_info)
		else:
			each_table_info.append(line)
	for each_table in all_tables_info:
		dict_table[each_table[0]]=each_table[1:]
	fh.close()
	return dict_table

all_tables_metadata=read_metadata("metadata.txt")

def read_csv(file):
	if not os.path.isfile(file):
		print("The file with name %s does not exist" %(file))
		exit()
	with open(file, 'r') as file:
		reader = csv.reader(file)
		data=[]
		for row in reader:
			b=[]
			for each_data in row:
				num=0
				for each_char in each_data:
					if each_char.isdigit():
						num=num*10
						num=num+int(each_char)
				b.append(num)
			row=b
			for each_index in range(len(row)):
				row[each_index]=str(row[each_index])
			data.append(row)
	return data

def select_all_query(table):
	tables_req=table[0].split(',')
	req_tables=deepcopy(table[0].split(','))
	if len(tables_req)==1:
		all_tables_data=[]
		tables_attr=[]
		for every_table in tables_req:
			if every_table in all_tables_metadata:
				for attr in range(len(all_tables_metadata[every_table])):
					tables_attr.append(every_table+"."+all_tables_metadata[every_table][attr])
			else:
		 		print("%s does not exist in the database" %(every_table))
		 		exit()
		all_tables_data.append(tables_attr)
		count=1
		for every_table in tables_req:
			row_data=read_csv(every_table+".csv")
			for each_row in range(len(row_data)):
				if each_row+1==len(all_tables_data):
					all_tables_data.append(row_data[each_row])
				else:
					all_tables_data[each_row+1].extend(row_data[each_row])
		return all_tables_data
	else:
		cols_req=[]
		for each_table in tables_req:
			if each_table in all_tables_metadata:
				cols_req.append(all_tables_metadata[each_table])
			else:
				print("%s is not present in the database"%(each_table))
		columns_diff_table=[]
		attr=[]
		row_data=[]
		num_col=0
		for each_table in tables_req:
			if each_table in all_tables_metadata:
				for each_col in cols_req[num_col]:
					attr.append(each_table+'.'+each_col)
				num_col=num_col+1
			else:
				print("%s is not present in the database"%(each_table))
		columns_diff_table.append(attr)
		for each_table in req_tables:
			row_data.append(read_csv(each_table+'.csv'))
		for element in itertools.product(*row_data):
			new_element=[]
			for each_element in element:
				new_element.extend(each_element)
			columns_diff_table.append(new_element)

		return columns_diff_table

def req_column_in_csv(file,col,req_table):
	if not os.path.isfile(file):
		print("The file with name %s does not exist" %(file))
		exit()
	flag=0
	if req_table not in all_tables_metadata:
		print("This table is not present in the database")
		exit()
	col_pos=0;
	for column in all_tables_metadata[req_table]:
		if column==col[0]:
			flag=1
			break
		col_pos=col_pos+1
	if flag==0:
		print("This column is not present in the given table")
		exit()
	with open(file, 'r') as file:
		reader = csv.reader(file)
		data=[]
		for row in reader:
			num=0
			for each_char in row[col_pos]:
				if each_char.isdigit():
					num=num*10
					num=num+int(each_char)
			row[col_pos]=str(num)
			data.append(row[col_pos])
	return data

def aggregate_query(keywords,table):
	req_table=table[0]
	req_str=keywords[1]
	col=[]
	flag=0
	temp_store=[]
	for req_substr in req_str:
		if req_substr==')':
			flag=0
		if flag==1:
			col.append(req_substr)
		if req_substr=='(':
			flag=1;
	if "max" in keywords[1]:
		column=req_column_in_csv(req_table+".csv",col,req_table)
		maximum=int(column[0])
		for data in column:
			if int(data)>=maximum:
				maximum=int(data)
		temp_store.append(["max"+'('+req_table+"."+col[0]+')'])
		temp_store.append([str(maximum)])
	if "min" in keywords[1]:
		column=req_column_in_csv(req_table+".csv",col,req_table)
		minimum=int(column[0])
		for data in column:
			if int(data)<=minimum:
				minimum=int(data)
		temp_store.append(["min"+'('+req_table+"."+col[0]+')'])
		temp_store.append([str(minimum)])
	if "sum" in keywords[1]:
		column=req_column_in_csv(req_table+".csv",col,req_table)
		sum_col=0
		for data in column:
			sum_col=sum_col+int(data)
		temp_store.append(["sum"+'('+req_table+"."+col[0]+')'])
		temp_store.append([str(sum_col)])
	if "avg" in keywords[1]:
		column=req_column_in_csv(req_table+".csv",col,req_table)
		average_col=0
		for data in column:
			average_col=average_col+int(data)
		average_col=average_col/len(column)
		temp_store.append(["avg"+'('+req_table+"."+col[0]+')'])
		temp_store.append([str(average_col)])
	if "distinct" in keywords[1]:
		column=req_column_in_csv(req_table+".csv",col,req_table)
		distinct_data=[]
		for data in column:
			if int(data) not in distinct_data:
				distinct_data.append(int(data))
		temp_store.append([req_table+"."+col[0]])
		for data in distinct_data:
			temp_store.append([str(data)])
	return temp_store
def ambiguous_column(column,table):
	freq_column=0
	for each_table in table:
		if each_table in all_tables_metadata:
			if column in all_tables_metadata[each_table]:
				freq_column=freq_column+1
		else:
			print("%s is not present in the database"%(each_table))
			exit()
	return freq_column

def select_columns(keywords,table):
	req_tables=table[0].split(',')
	cols_req=keywords[1].split(',')
	for colu in cols_req:
		if ambiguous_column(colu,req_tables)>1:
			print("The column %s is ambiguous"%(colu))
			exit()
	for each_table in req_tables:
		count_col=0
		if each_table in all_tables_metadata:
			for colus in cols_req:
				if colus in all_tables_metadata[each_table]:
					count_col=count_col+1
		else:
			print("The %s is not present in the database"%(each_table))
		if count_col==len(cols_req):
			new_req_tables=[each_table]
			req_tables=new_req_tables
			break

	if len(req_tables)==1:
		cols_req=keywords[1].split(',')
		columns_one_table=[]
		attr=[]
		if req_tables[0] in all_tables_metadata:
			for cols in cols_req:
				if cols in all_tables_metadata[req_tables[0]]:
					attr.append(req_tables[0]+'.'+cols)
				else:
					print("The column %s is not present in the table" %(cols))
					exit()
		else:
			print("%s is not present in the database" %(req_tables[0]))
			exit()
		columns_one_table.append(attr)
		for cols in cols_req:
			columns_one_table.append(req_column_in_csv(req_tables[0]+'.csv',cols,req_tables[0]))
		new_columns_one_table=[]
		new_columns_one_table.append(attr)
		len_column=len(columns_one_table[1])
		index_array=1
		index_row=0
		while index_row<=len_column-1:
			index_array=1
			temp_items=[]
			while index_array<=len(columns_one_table)-1:
		 		temp_items.append(columns_one_table[index_array][index_row])
		 		index_array=index_array+1
			index_row=index_row+1
			new_columns_one_table.append(temp_items)
		return new_columns_one_table
	else:
		cols_req=keywords[1].split(',')
		columns_diff_table=[]
		attr=[]
		column_data=[]
		for each_col in cols_req:
			flag=0
			for each_table in req_tables:
				if each_table in all_tables_metadata:
					if each_col in all_tables_metadata[each_table]:
						flag=1
						attr.append(each_table+'.'+each_col)
						column_data.append(req_column_in_csv(each_table+'.csv',each_col,each_table))
						break
				else:
					print("This table is not present in database")
					exit()
			if flag==0:
				print("The colum %s is not present in the requested tables"%(each_col))
				exit()
		columns_diff_table.append(attr)
		for element in itertools.product(*column_data):
			columns_diff_table.append(element)
		return columns_diff_table

def compare_true(val1,op,val2):
	if op=='<=':
		if int(val1)<=int(val2):
			return True 
		else:
			return False
	if op=='>=':
		if int(val1)>=int(val2):
			return True
		else:
			return False
	if op=='=':
		if int(val1)==int(val2):
			return True
		else:
			return False
	if op=='<':
		if int(val1)<int(val2):
			return True
		else:
			return False
	if op=='>':
		if int(val1)>int(val2):
			return True
		else:
			return False

def select_from_where_query(keywords,table):
	actual_data=select_columns(keywords,table)
	req_columns=keywords[1].split(',')
	where_conditions=keywords[4].split(' ')
	for operator in relational_operators:
		if operator in where_conditions[1]:
			operator1=operator
			condition_1=where_conditions[1].split(operator)
			break
	condition_data=[]
	if len(where_conditions)>2:
		for operator in relational_operators:
			if operator in where_conditions[3]:
				operator2=operator
				condition_2=where_conditions[3].split(operator)
				break
		condition=where_conditions[2]
		operators=[operator1,operator2]
		conditioned_columns=[condition_1[0],condition_2[0]]
		conditioned_values=[condition_1[1],condition_2[1]]
		index_columns=[req_columns.index(colum) for colum in conditioned_columns]
		j=1
		condition_data=[]
		condition_data.append(actual_data[0])
		while j<len(actual_data):
			if condition=="AND" or condition=="and":
				if compare_true(actual_data[j][index_columns[0]],operators[0],conditioned_values[0]):
					if compare_true(actual_data[j][index_columns[1]],operators[1],conditioned_values[1]):
						condition_data.append(actual_data[j])
			if condition=="OR" or condition=="or":
				if compare_true(actual_data[j][index_columns[0]],operators[0],conditioned_values[0]):
					condition_data.append(actual_data[j])
				elif compare_true(actual_data[j][index_columns[1]],operators[1],conditioned_values[1]):
					condition_data.append(actual_data[j])
			j=j+1
	elif len(where_conditions)==2:
		j=1
		condition_data.append(actual_data[0])
		temp_index=req_columns.index(condition_1[0])
		while j<len(actual_data):
			if compare_true(actual_data[j][temp_index],operator1,condition_1[1]):
				condition_data.append(actual_data[j])
			j=j+1
	else:
		print("This query is invalid")
		exit()
	return condition_data					

def join_query(keywords,table):
	new_keywords=keywords[0:4]
	required_tables=table[0].split(',')
	columns_req=keywords[1].split(',')
	for colu in columns_req:
		if ambiguous_column(colu,required_tables)>1:
			print("The column %s is ambiguous"%(colu))
			exit()
	if len(required_tables)<=1:
		print("The query is invalid because only one table is present in the from condition")
		exit()
	join_keywords=keywords[4].split(' ')
	for op in relational_operators:
		if op in join_keywords[1]:
			oper=op
			break
	req_cols_tables=join_keywords[1].split(oper)
	table1_col1=req_cols_tables[0].split('.')
	table2_col2=req_cols_tables[1].split('.')
	if table1_col1[1] not in all_tables_metadata[table1_col1[0]]:
		print("%s is not in %s"%(table1_col1[1],table1_col1[0]))
		exit()
	elif table2_col2[1] not in all_tables_metadata[table2_col2[0]]:
		print("%s is not in %s"%(table2_col2[1],table2_col2[0]))
		exit()	
	req_data=[]
	req_data=select_all_query(table)
	new_join_data=[]
	req_index=[req_data[0].index(req_cols_tables[0]),req_data[0].index(req_cols_tables[1])]
	new_join_data.append(req_data[0])
	if len(join_keywords)==2:
		j=1
		while j<len(req_data):
			if compare_true(req_data[j][req_index[0]],oper,req_data[j][req_index[1]]):
				new_join_data.append(req_data[j])
			j=j+1
	elif len(join_keywords)>2:
		logic_op=join_keywords[2]
		req_op=[]
		for op in relational_operators:
			if op in join_keywords[3]:
				req_op=op
				break
		last_split=join_keywords[3].split(req_op)
		last_req_index=req_data[0].index(last_split[0])
		if logic_op=='AND' or logic_op=='and':
			j=1
			while j<len(req_data):
				if compare_true(req_data[j][req_index[0]],oper,req_data[j][req_index[1]]):
					#print(compare_true(req_data[j][last_req_index],req_op,last_split[1]))
					if compare_true(req_data[j][last_req_index],req_op,last_split[1]):
						new_join_data.append(req_data[j])
				j=j+1
		if logic_op=="OR" or logic_op=="or":
			j=1
			while j<len(req_data):
				if compare_true(req_data[j][req_index[0]],oper,req_data[j][req_index[1]]):
					new_join_data.append(req_data[j])
				elif compare_true(req_data[j][last_req_index],req_op,last_split[1]):
					new_join_data.append(req_data[j])
				j=j+1
	final_new_join_data=[]
	j=0
	if keywords[1]=='*':
			if table1_col1[1]==table2_col2[1]:
				while j<len(new_join_data):
					each_data_ind=0
					temp_each_data=[]
					while each_data_ind<len(new_join_data[j]):
						if each_data_ind!=req_index[0]:
							temp_each_data.append(new_join_data[j][each_data_ind])
						each_data_ind=each_data_ind+1
					final_new_join_data.append(temp_each_data)
					j=j+1
				new_join_data=deepcopy(final_new_join_data)	
	else:
			req_columns_index=[]
			for each_col in columns_req:
				for each_table in all_tables_metadata:
					if each_col in all_tables_metadata[each_table]:
						req_columns_index.append(each_table+'.'+each_col)
						break
			new_index_req=[req_data[0].index(each_index) for each_index in req_columns_index]
			j=0
			while j<len(new_join_data):
					each_data_ind=0
					temp_each_data=[]
					while each_data_ind<len(new_join_data[j]):
						if each_data_ind in new_index_req:
							temp_each_data.append(new_join_data[j][each_data_ind])
						each_data_ind=each_data_ind+1
					final_new_join_data.append(temp_each_data)
					j=j+1
			new_join_data=deepcopy(final_new_join_data)	
	return new_join_data

def execute_commands(keywords):
	flag=0
	query_data=[]
	if(len(keywords)<4):
		print("The query is invalid")
		exit()
	if len(keywords)==4:
		table_name=keywords[3:]    #change this line if there is semicolon at the end
		if keywords[1]=="*":
			query_data=select_all_query(table_name)
		else:
			for aggregate_word in aggregates:
				if aggregate_word in keywords[1]:
					flag=1
					query_data=aggregate_query(keywords,table_name)
					break
			if flag==0:
				query_data=select_columns(keywords,table_name)
	else:
		table_name=keywords[3:4]
		temp_req_tables=table_name[0].split(',')
		join_condition=0
		for each_table in temp_req_tables:
			if each_table in keywords[4]:
				join_condition=1
				break
		if join_condition==0:
			query_data=select_from_where_query(keywords,table_name)
		else:
			query_data=join_query(keywords,table_name)
	return query_data


def split_query(argument):
	query=sqlparse.parse(argument)[0].tokens
	get_keywords=sqlparse.sql.IdentifierList(query).get_identifiers()
	keywords=[]
	for command in get_keywords:
		keywords.append(str(command))
	if keywords[0]=="select":
		final_data=execute_commands(keywords)
		for every_row in range(len(final_data)):
			print (','.join(final_data[every_row]))
	else:
		print("Your query is not supported by my sql_engine")
		exit()

split_query(argument)