##包结构
</br>├── README.md
</br>├── dao.go
</br>├── dao_impl.go
</br>├── dao_pg2mysql_cron.go
</br>├── dao_pg2mysql_impl.go
</br>├── dao_rep_extra.go
</br>├── dao_test.go
</br>├── db.go
</br>├── orm
</br>│   └── topview.go
</br>├── pg
</br>│   ├── const.go
</br>│   ├── cron.go
</br>│   ├── dao.go
</br>│   ├── dao_impl.go
</br>│   ├── db.go
</br>│   ├── wire.go
</br>│   └── wire_gen.go
</br>├── rpc
</br>├── wire.go
</br>└── wire_gen.go

##特殊业务说明
###1.pg包及带有pg2mysql的go文件
适配老版财务数据导出逻辑，从pg库源导出到mysql中
###2.dao_rep_extra.go
cs/hxfinance中的特殊逻辑，部分财报数据根据起所在财报类型加载到其对应拓展id  
######拓展id规则如下：  
报表类型的掩码： 0x00070000  
一季报：0x00010000  
中报：0x00020000   
三季报：0x00030000    
年报：0x00040000  

######eg.619与262763  
#######对应关系
原字段：净利润——619 拓展字段：年报净利润——262763  
原字段ID、拓展字段ID、报表类型转换关系如下所示：  
262763 = 619｜0x00040000  
0x00040000 = 262763&0x00070000  
619 = 262763^0x00040000
262763的数据即为619的数据中财报期为年报的数据，即日期为1231的数据  
以此类推，一季报、中报、三季报拓展分别为0331，630和931
#######更新机制
cs/hxfinance老机制：    
每次原字段更新时清理对应拓展字段的数据池并重新填充  
hxextract适配机制：  
每次原字段所在表更新时同时更新拓展字段表对应更新，根据上述逻辑，可用以下sql更新：  
replace into JlrReportYear select code,datetime,isvalid,`src-time`,`master-time`,jlr as `jlr_rep_year` from ProfitSharing where datetime%10000 = 1231]
其他类似  
注：目前主行情有该特殊逻辑需求的仅619/262763
