#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from datetime import timedelta, datetime

import re
import traceback
from os.path import join
from os import listdir

from clsearch import ColumnsSearch


# In[2]:


def gen_df_case(df, _id, _visit):
  neyear=timedelta(days=455)

  case_list = pd.read_csv('CaseList.csv', usecols=['CL_CASE_TYPE','CL_PatientID', 'CL_CASE_DATE'],
                         parse_dates = ['CL_CASE_DATE'], 
                         date_parser=lambda x:pd.to_datetime(x,format='%Y%m%d',errors='coerce'), 
                         encoding = 'unicode_escape')
  
#先filter(減少後續搜索時間)
#first filter time
#直接用min_visit_date - 455當作所以case第一時間(才差一個月，只會多抓幾個case)
#date after 20200709(用api抓)
  yrago = df[_visit].min()-neyear
  case_list = case_list[(case_list['CL_CASE_DATE']>=yrago)&(case_list['CL_CASE_DATE']<datetime(2022,7,9))]
    
#second filter id
  case_list = case_list[case_list['CL_PatientID'].isin(pd.unique(df[_id]))]

#rename columns before groupby
  case_list.rename(columns={'CL_CASE_DATE': 'time', 'CL_CASE_TYPE': 'case'}, inplace = True)
    
#drop 'time'or'case' is na(後續補上就好)
  case_list.dropna(subset = ['time', 'case'], inplace = True)
  
#groupby
  grouped = case_list.groupby('CL_PatientID')
  data = {_id: [], 'group': []}
  for k in grouped.indices:
    data[_id].append(k)
    data['group'].append(grouped.get_group(k)[['time','case']])

  grouped_df = pd.DataFrame.from_dict(data)

#merge by id: df(id, visit), grouped_df(id, get_grouped)

  df_case = df.merge(grouped_df, how='left', on=_id, validate='m:1')
  df_case.set_index([_id, _visit], inplace=True)
  case_dict = {k: [v['group']] for k, v in df_case.to_dict('index').items()}
#case_dict: {(id, visit): [df]...}
  return yrago, case_dict


# In[4]:


ad_year=relativedelta(years=1911)

#對耀聖方案，有錯者另外拉成newdf
#不要動到原始case_dict裡的grouped_df
def CasePair(yrago, case_dict):

  sixday=timedelta(days=6)

#dict for inner_casecompair
  compare_dict={'is1408CKD':['p4302c.xls','p1408c.xls'], 'is1409CKD':['p4302c.xls','p1409c.xls'],
            'is4302CKD':['p4301c.xls','p1408c.xls'], 'is4301CKD':['p4301c.xls'],
            'is1408':['p1408c.xls'], 'is1409':['p1409c.xls'], 
            'is7001':['p7001c.xls'], 'is7002':['p7002c.xls'], 
            'not1408':['n1408c.xls'], 'not1409':['n1409c.xls'], 
            'is1407':['p1407c.xls'],'not1407':['n1407c.xls']}

#抓日期區間
  _strfmt='\d{3}.\d{2}.\d{2}'
    
  def _compair(g, _id, pair_dict):
    
#sort by time
    g = g.sort_values(by=['time'])
    
    def inner_compair(arrlike, _tm, pair_dict, _case=None, oldcase=None):
        
#每進到inner_compair一次就copy一次，不然一次apply會用到同一個copy，上一列如果有pop，後一列會被影響
      pairdict = pair_dict.copy()
      nonlocal newdf, g
#arrlike[arg]就算改動g也不會變        
      if arrlike[_tm] < StDate or arrlike[_tm] > EnDate:
        return
    
#一開始是輸入_case, 進入recursion是輸入oldcase.        
#如果_case+, oldcase=None，代表是一開始。oldcase = arrlike['case']，ct_k=arrlike[_case]
      if _case:
        
        oldcase = arrlike[_case]

        if oldcase not in pairdict:
          return
        ct_k = oldcase
#如果未輸入_case，代表innercompair recursion，ct_k=next key，oldcase = oldcase(不用再assign)
      else:
        ct_k = list(pairdict.keys())[0]

#if case start with not, compair only after 20220301(以後可以刪)
      if re.match('not', ct_k) and arrlike[_tm]<datetime(2022,3,1):
        return
    
#set index(original index) to idx(do not change index during change g)
      idx = arrlike.name

#pop key
      for ct in pairdict.pop(ct_k):
        
    
#都先插入dummies，之後要比對比較簡單
#if ct not in g dummies and chartdict[ct] not empty, insert dummies        
        if ct not in g.columns:
   
          df_ct = chartdict[ct][(chartdict[ct]['id']==_id) & (chartdict[ct]['看診日期']>=yrago)]
          if not df_ct.empty:

#time>=ct(6天內)新merge才有'看診日期'
#merge_asof is left-join(need reindex)

            g = pd.merge_asof(g, df_ct, left_on=_tm, right_on='看診日期', tolerance=sixday).set_index(g.index)
            
#update g with看診日期(如果有對到才會update)，沒對到不會動到
            g[_tm].update(g['看診日期'])
            
#if df_ct not not a subset of g, then merge use outer
#use oversize to create new index
            oversize = len(set(df_ct['看診日期'].dropna())-set(g[_tm]))
            if oversize:
              
#first drop看診日期for overlapping columns(舊看診日期已經update到g['time'],可以刪)
              g.drop(columns='看診日期', inplace=True)
              newindex = g.index.union(pd.Index([g.index[-1]+i for i in range(1, oversize+1)]))
              g = g.merge(df_ct['看診日期'], 
                          left_on=_tm, right_on='看診日期', 
                          how='outer').set_index(newindex)
              
              g[_tm].update(g['看診日期'])
              
              g.sort_values(by=_tm, inplace=True)
#case fillna with err
              g[['case']] = g[['case']].fillna('err')
              
            g.rename(columns={'看診日期': ct}, inplace = True)
#刪id cl
            g.drop(columns = 'id', inplace = True)
#if any ct not found(no g.columns or is None), move to next pair_dict(now key is already popped)
       
        if ct not in g.columns or pd.isna(g.at[idx, ct]):
            
#if pairdict未空則進入比對
          if pairdict:
            inner_compair(arrlike=arrlike, _tm=_tm, pair_dict=pairdict, oldcase=oldcase)
#if pairdict為空，代表沒有匹配到, case改成'err'
          else:
            g.at[idx, 'case'] = 'err'

            newdf=pd.concat([newdf, pd.DataFrame(data={'id':[_id],'time':[arrlike[_tm]],
                                                       'oldcase':[oldcase],'newcase':['err']})])
          break
          
     
#if ct_k is the true case(無論在何層)(means該層 no break), compair to case, if not case, write into newdf
      else:
        
        if ct_k!=oldcase:
          g.at[idx, 'case'] = ct_k

          newdf=pd.concat([newdf, pd.DataFrame(data={'id':[_id],'time':[arrlike[_tm]],
                                                     'oldcase':[oldcase],'newcase':[ct_k]})])
#apply一開始就會設定好範圍，後續增加者不會apply
    g.apply(inner_compair, _tm='time', pair_dict=pair_dict, _case='case', axis=1)

    return g
#CasePair start...
#open newdf
  newdf = pd.DataFrame(columns = ['id','time','oldcase','newcase']) 

#先把chartfolder裡檔案都打開，add to chartdict
  chartdict={}

#檔名通通轉小寫(連igt結案一起讀)
  for _f in listdir('casefolder'):

    chartdict[_f.lower()]=pd.read_excel(join('casefolder', _f))

  StDate, EnDate=tuple(re.findall(_strfmt, chartdict['p1407c.xls'].columns[0]))
#抓StDate, EnDate
  StDate, EnDate = parse(StDate)+ad_year, parse(EnDate)+ad_year
    
#抓看診日期和身分證字號row and cl index
  rw, cl_list=ColumnsSearch(chartdict['p1407c.xls'], ['身份證字號','看診日期'])
    
#get all id
  ids, _ = zip(*case_dict.keys())
    
#crop the chartdict, reset column names(***index not reset) and date
  for d in chartdict:
    chartdict[d] = chartdict[d].iloc[rw:, cl_list]
    chartdict[d].columns = ['id','看診日期']
    chartdict[d].dropna(inplace = True)
    chartdict[d].drop(chartdict[d].index[0], inplace = True)
    chartdict[d]['看診日期'] = chartdict[d]['看診日期'].apply(lambda x: parse(x) + ad_year)
#(use grouped id to filter chartdict[d],減少後續搜索時間)
    chartdict[d] = chartdict[d][chartdict[d]['id'].isin(ids)]

#k[0]=id, k[1]=visit, v[group]=g=>change to {(id, visit):[grouped_df]}
  case_dict = {k: [v[0].pipe(_compair, _id=k[0], pair_dict=compare_dict) if isinstance(v[0], pd.DataFrame)
                   else v[0]] for k, v in case_dict.items()}

#check if igt not missing and try change to int 1 if not
  check_igt = lambda arrlike, t_x, igt_nearest: 1 if arrlike[t_x]-igt_nearest<=sixday else pd.NA
    
#抓igt, ckd結案, dm結案時間    
  for k,_g in case_dict.items():

    if isinstance(_g[0], pd.DataFrame):
#抓igt max, create igt cl
      igt_nearest = chartdict['igt.xls'].loc[chartdict['igt.xls']['id']==k[0], '看診日期'].max()
#不見得比較快，但比較乾淨('time'有誤差未改，但因是igt方案應該沒差)
      _g[0]['igt'] = _g[0].apply(check_igt, axis=1, t_x='time', igt_nearest=igt_nearest)
    
#[]append ckd結案
      _g.append(chartdict['ckd結案.xls'].loc[chartdict['ckd結案.xls']['id']==k[0], '看診日期'].max())
#[]append dm結案    
      _g.append(chartdict['dm結案.xls'].loc[chartdict['dm結案.xls']['id']==k[0], '看診日期'].max())
    
  newdf.to_csv('casepair.csv', index=False) 

  return case_dict     


# In[ ]:




