#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
from multiprocessing import Pool
import functools


# In[ ]:


raw = pd.read_csv('comparative.ai_take_home_test.csv',index_col=['user_id','country','is_vip'])


# In[ ]:


c_list = list(raw.reset_index()['country'].unique())


# In[ ]:


def total_purchased_amount(df):
    return df.sum()#['purchased_amount_today']


# In[ ]:


overall_total=total_purchased_amount(raw)


# In[ ]:


overall_total


# In[ ]:


total_change=abs((overall_total[1]-overall_total[0])/overall_total[0])


# In[ ]:


def total_purchased_amount_mean(df):
    return df[df['purchased_amount_today']>0].mean()


# In[ ]:


overall_total_mean=total_purchased_amount_mean(raw)


# In[ ]:


total_mean_change=abs((overall_total_mean[1]-overall_total_mean[0])/overall_total_mean[0])


# In[ ]:


def func(raw,c):
    
    df_all = raw.loc[(slice(None),c,slice(None)),:]
    df_not_vip = raw.loc[(slice(None),c,False),:] 
    df_vip = raw.loc[(slice(None),c,True),:]
    
    return (c,total_purchased_amount(df_all),total_purchased_amount_mean(df_all),total_purchased_amount_mean(df_not_vip),total_purchased_amount_mean(df_vip))


# In[ ]:

f=functools.partial(func,raw)



# In[ ]:


#totsmc=pd.DataFrame(res).applymap(lambda x:f'${x[1]:.2f} Change: {(x[1]-x[0])*100/x[0]:.0f}% from yesterday' if abs((x[1]-x[0])/x[0])>total_mean_change else 'Not Significant')


# In[ ]:


#totsc=pd.DataFrame(res).applymap(lambda x:f'${x[1]:.2f} Change: {(x[1]-x[0])*100/x[0]:.0f}% from yesterday' if abs((x[1]-x[0])/x[0])>total_change else 'Not Significant')


# In[ ]:


#totsmc.index=['Total','Mean','Mean not VIP','Mean VIP']
#totsc.index=['Total','Mean','Mean not VIP','Mean VIP']


# In[ ]:


#tots=totsmc


# In[ ]:


#tots.loc[slice('Total'),:]=totsc.loc[slice('Total'),:]


# In[ ]:


#tots


# In[ ]:
if __name__=='__main__':

    with Pool(processes=4) as pool:
        res_l=pool.map(f,c_list)
    print(res_l)

