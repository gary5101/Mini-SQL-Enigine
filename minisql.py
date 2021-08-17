import csv
import sys
import sqlparse
from moz_sql_parser import parse
from itertools import product, chain
import pprint

DATA_DIR=''

schema={}
table_names=[]
db={}

def load_data():

    f = open(DATA_DIR+'metadata.txt',"r")
    metadata = f.readlines()
    metadata = [i.rstrip().lower() for i in metadata ]

    i = 0
    while i < len(metadata):
        if metadata[i] == "<begin_table>":
            table_names.append(metadata[i+1])
            schema[table_names[-1]]=[metadata[i+2]]
            i+=3
        elif metadata[i] == "<end_table>":
            i+=1
        else:
            schema[table_names[-1]].append(metadata[i])
            i+=1
    # print(schema)
    for i in table_names:
        with open(DATA_DIR+i+'.csv') as t:
            csv_reader = csv.reader(t, delimiter=',')
            db[i]={'cols':schema[i],'data':[]}
            
            for j in csv_reader:
                k=[int(x) for x in j]
                db[i]['data'].append(k)
    # print(db)

def sqlprint(reqDb, function):
    firstrow=""
    for i,c in enumerate(reqDb['cols']):
        colname=""
        for j in schema.keys():
            if c in schema[j]:
                colname=j+'.'+c
                break
        if c == function[1]:
            colname=function[0]+'('+colname+')'
        firstrow+=colname+', '
    print(firstrow[0:-2])
    
    for i in reqDb['data']:
        row=''
        for j in i:
            row+=str(j)+', '
        print(row[0:-2])
            
def remove_blanks(old):
    new = [i for i in old if i.normalized != " "]
    return new

def check_distinct(query):
    if query.normalized =="DISTINCT":
        return True
    else:
        return False

def get_cols_and_tables(query,k):
    try:
        if (isinstance(query[k],sqlparse.sql.Identifier) | isinstance(query[k],sqlparse.sql.IdentifierList)|(query[k].normalized=='*')| isinstance(query[k],sqlparse.sql.Function)) \
            & ( isinstance(query[k+2],sqlparse.sql.Identifier) | isinstance(query[k+2],sqlparse.sql.IdentifierList))\
            &(query[k+1].normalized=="FROM"):
            return query[k], query [k+2]

        else:
            raise Exception

    except Exception:
        print("invalid query")
        exit()
def extract_reqdata(tableNames):
    reqDb = {'cols':[],'data':[]}
    try:
        if len(tableNames)==0:
            raise Exception

        for i in tableNames:
            reqDb['cols']=list(chain(reqDb['cols'],db[i]['cols']))

        data = [db[i]['data'] for i in tableNames]
        temp=(product(*data))
        totalData = []
        for i in temp:
            totalData.append(list(chain(*list(i))))
        reqDb['data']=totalData

        return reqDb
            
    except:
        print("invalid query, check table names")
        exit()

def evalWhere(condition, op, reqDb):
    
    op = "&" if op=='AND' else "|" if op=="OR" else ""
    def checkVal(val):
        try:
            val = float(val)
        except Exception:
            pass
        return val

    def getVal(x, row): 
        return row[reqDb['cols'].index(x)] if isinstance(x, str) else x

    reqRows = []
    for row in reqDb['data']:
        
        # print(str(getVal(checkVal(condition[0][0].normalized),row)),str(getVal(checkVal(condition[0][2].normalized),row)))
        eval_stat=""
        try:
            if op =="":
                innerop = '==' if condition[0][1].normalized=="=" else condition[0][1].normalized
                # print(innerop)
                eval_stat = str(getVal(checkVal(condition[0][0].normalized),row))+innerop+str(getVal(checkVal(condition[0][2].normalized),row))
                # print(eval_stat)
                reqRows.append(eval(eval_stat))
            
            else:
                for c in condition:
                    innerop = '==' if c[1].normalized=="=" else c[1].normalized
                    temp = str(getVal(checkVal(c[0].normalized),row))+innerop+str(getVal(checkVal(c[2].normalized),row))
                    eval_stat+="("+temp+")"
                    eval_stat+=op
                    
                reqRows.append(eval(eval_stat[0:-1]))
                
        except:
            print("Error: Invalid Query")
            exit()    
    return reqRows

def re_eval_reqDb(reqDb,reqRows):
    newData=[]
    for i,row in enumerate(reqDb['data']):
        if reqRows[i]==True:
            newData.append(row)
    reqDb['data']=newData
    return reqDb

def transpose(l1, l2):
 
    # iterate over list l1 to the length of an item 
    for i in range(len(l1[0])):
        # print(i)
        row =[]
        for item in l1:
            # appending to new list with values and index positions
            # i contains index position and item contains values
            row.append(item[i])
        l2.append(row)
    return l2

def handle_groupby(reqDb,groupcolname,function):
    indx = reqDb['cols'].index(groupcolname)
    groupCol = [i[indx] for i in reqDb['data']]
    funccol=None
    func=None
    if function !=None:
        funccol= function[1]
        func=function[0]
        findx = reqDb['cols'].index(funccol)
    # print(indx)
    checklist = {key[indx]:reqDb['cols'].copy() for key in reqDb['data']}
    # pprint.pprint(checklist)

    for i, row in enumerate(reqDb['data']):
        # print(i,row,checklist[row[indx]])
        # pprint.pprint(checklist)

        for j,col in enumerate(checklist[row[indx]]):

            # print(j,col,row[indx])
            if isinstance(col,str):
                # print("hi")
                checklist[row[indx]][j]=row[j]
                if function!=None:
                    if j ==findx:
                        if func=='avg':
                            checklist[row[indx]][j]=(row[j],1)
                        elif func=='count':
                            checklist[row[indx]][j]=1
                

            elif function !=None:
                if j == findx:
                    if func == 'avg':
                        checklist[row[indx]][j] = (checklist[row[indx]][j][0]+row[j],checklist[row[indx]][j][1]+1)
                    elif func == 'min':
                        checklist[row[indx]][j] = min(checklist[row[indx]][j],row[j])
                    elif func == 'max':
                        checklist[row[indx]][j] = max(checklist[row[indx]][j],row[j])
                    elif func == 'sum':
                        checklist[row[indx]][j] = checklist[row[indx]][j]+row[j]
                    elif func == 'count':
                        checklist[row[indx]][j] = checklist[row[indx]][j]+1
                
                else:
                    # print(checklist[row[indx]][j],row[j])
                    if checklist[row[indx]][j] != row[j]:
                        checklist[row[indx]][j] = None
            
            else:
                if checklist[row[indx]][j] != row[j]:
                        checklist[row[indx]][j] = None

            
        # print("\n\n\n")

    # pprint.pprint(checklist)

    newreqDb={'cols':[],'data':[]}
    for i in reqDb['cols']:
        ind = reqDb['cols'].index(i)
        
        flag = True
        for j, colval in enumerate(checklist.keys()):
            if checklist[colval][ind] == None:
                flag=False

        if flag:
            newreqDb['cols'].append(i)
    
    for i in newreqDb['cols']:
        ind = reqDb['cols'].index(i)
        newrow=[]
        for j, colval in enumerate(checklist.keys()):
            newrow.append(checklist[colval][ind])

        newreqDb['data'].append(newrow)
    
    newreqDb['data']= transpose(newreqDb['data'],[])

    if function!=None:
        if func == 'avg':
            findx = newreqDb['cols'].index(funccol)
            for i,r in enumerate(newreqDb['data']):
                newreqDb['data'][i][findx] = newreqDb['data'][i][findx][0]/newreqDb['data'][i][findx][1]

    return newreqDb

        
def handle_orderby(reqDb,ordercolname,function,group,ordertype):
    if function!=None and group==True:
        if function[1]==ordercolname:
            print("error")
            exit()
    if ordertype == 'asc':
        idx = reqDb['cols'].index(ordercolname)
        check = sorted(reqDb['data'],key=lambda x: float(x[idx]))
    else:
        idx = reqDb['cols'].index(ordercolname)
        check = sorted(reqDb['data'],key=lambda x: float(x[idx]),reverse=True)
        
    newreqDb={'cols':reqDb['cols'],'data':check}
    return newreqDb

def findfunctions(colNames,allcols):
    function = None
    allfunction = ['avg', 'max','min','sum','count']
    for col in colNames:
        if function!=None:
            print("enter only one aggregate func")
            exit()
        if isinstance(col, sqlparse.sql.Function):
            part = col.normalized[0:-1].split('(')
            part = [ i.strip() for i in part]
            if part[0].lower() not in allfunction:
                print("invalid function")
                exit()
            
            if part[1] not in allcols:
                print("invalid names")
                # print(part[1])
                exit()
            function = (part[0].lower(),part[1])
            return function

def get_colNames(cols):
    colNames=[]
    for col in cols:
        if isinstance(col,sqlparse.sql.Identifier):
            colNames.append(col.normalized)
        elif isinstance(col,sqlparse.sql.Function):
            colNames.append(col.normalized[0:-1].split('(')[1])
    
    return colNames

def parse_query(query):
    
    try:
        query = sqlparse.parse(query)[0]
    except:
        print("Parsing failed")
        exit()
    parsed=None
    try:
        if query[0].normalized =="SELECT" and query[-1].normalized[-1] == ";":
            parsed = remove_blanks(query)

        else:
            raise Exception
    except Exception:
        print("invalid syntax")
        exit()

    distinct = check_distinct(parsed[1])

    k = 1 if distinct == False else 2
    

    cols, tables = get_cols_and_tables(parsed,k)
    # print(cols)
    tableNames=[]
    for i in tables:
        if isinstance(i,sqlparse.sql.Identifier):
            tableNames.append(i.normalized)
    
    if isinstance(tables,sqlparse.sql.Identifier):
        tableNames=[tables.normalized]


    colNames=[]
    if isinstance(cols, sqlparse.sql.IdentifierList):
        for i in cols:
            if isinstance(i,sqlparse.sql.Identifier) | isinstance(i,sqlparse.sql.Function):
                colNames.append(i)
            if i.normalized=='*':
                print("invalid select statement")
                exit()
    else:
        colNames=[cols]
    
    

    reqDb = extract_reqdata(tableNames)
    allcols = reqDb['cols']
    function=findfunctions(colNames,reqDb['cols'])

    reqRows=[True for _ in range(len(reqDb['data'])) ]
    group =False
    try:
        for i in range(k+3,len(parsed)):
            # print(i)
            if isinstance(parsed[i],sqlparse.sql.Where):
                whereParse = remove_blanks(parsed[i])[1:]
                if whereParse[-1].normalized==';':
                    whereParse = whereParse[:-1]

                if len(whereParse)==1:
                    newWhereParse = remove_blanks(whereParse[0])
                    reqRows = evalWhere([newWhereParse],"",reqDb)

                else:
                    newWhereParse1 = remove_blanks(whereParse[0])
                    newWhereParse2 = remove_blanks(whereParse[2])
                    reqRows = evalWhere([newWhereParse1,newWhereParse2],whereParse[1].normalized,reqDb)

                reqDb = re_eval_reqDb(reqDb,reqRows)
                # pprint.pprint(reqDb)

            elif parsed[i].normalized=="GROUP BY":
                if not (isinstance(parsed[i+1],sqlparse.sql.Identifier)):
                    raise Exception
                groupcol = parsed[i+1].normalized
                if groupcol not in reqDb['cols']:
                    raise Exception
                group =True
                reqDb=handle_groupby(reqDb,groupcol,function)
                # pprint.pprint(reqDb)
                

            elif parsed[i].normalized=="ORDER BY":
                
                if not (isinstance(parsed[i+1],sqlparse.sql.Identifier)):
                    # print('hi')
                    raise Exception
                ordercol = parsed[i+1].normalized
                # print(reqDb['cols'],ordercol)
                ordercol = ordercol.split(' ')
                # print(ordercol)
                ordertype = "asc"
                if len(ordercol)==1:
                    ordercol=ordercol[0]
                elif len(ordercol)==2:
                    ordertype=ordercol[1] 
                    ordercol=ordercol[0]
                else:
                    raise Exception

                if (ordertype!="asc") and (ordertype!="desc"):
                    raise Exception
                
                if ordercol not in reqDb['cols']:
                    # print("hi")
                    raise Exception
                # print("hi")
                reqDb=handle_orderby(reqDb,ordercol,function,group,ordertype)
                # pprint.pprint(reqDb)

        if (group==False) and (function!=None):
            funccol=function[1]
            func = function[0]
            findx = reqDb['cols'].index(funccol)
            aggCol = [i[findx] for i in reqDb['data']]
            
            answer=None

            if func == 'avg':
                answer= sum(aggCol)/len(aggCol)
            elif func == 'min':
                answer= min(aggCol)
            elif func == 'max':
                answer= max(aggCol)
            elif func == 'sum':
                answer= sum(aggCol)
            elif func == 'count':
                answer= len(aggCol)

            reqDb={'cols':[funccol],'data':[[answer]]}
            # pprint.pprint(reqDb)
        
        
        if colNames[0].normalized=="*":
            colnames=allcols
        else:
            colnames=get_colNames(colNames)
        

        for col in colnames:
            if col not in reqDb['cols']:
                raise Exception
        
        
        newreqDb = {'cols':colnames, 'data':[]}

        temp=[]
        for col in colnames:
            idx = reqDb['cols'].index(col)
            col_req = [i[idx] for i in reqDb['data']]
            temp.append(col_req)
            newreqDb['data']=transpose(temp,[])
        
        reqDb=newreqDb
        # pprint.pprint(reqDb)
        
        if distinct ==True:
            newreqDb = {'cols':colnames, 'data':[]}
            for i in reqDb['data']:
                if i not in newreqDb['data']:
                    newreqDb['data'].append(i.copy())
            reqDb=newreqDb
            # pprint.pprint(newreqDb)
        # pprint.pprint(reqDb)

        sqlprint(reqDb,function)
        # print(schema)
        
    except Exception:
        print("invalid query")
        exit()
           

def main():
    load_data()
    if len(sys.argv)!=2:
        print("invalid syntax")
        exit(-1)
    query = sys.argv[1].lower()
    if query.strip()[-1]!=";":
        print("invalid syntax")
        exit(-1)
    parse_query(query.strip())

if __name__=="__main__":
    main()