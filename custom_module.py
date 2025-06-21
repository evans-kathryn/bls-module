
import getpass
import datetime
import oracledb
import pandas as pd
import os 
import random


# status="dev"
status="prod"

sched_list=""
cycle=""

def get_status():
    return status

def get_output_loc():
    return output_loc


header={         
        "admin_info": "General Info",
        "resp_info": "Respondent Info",
        "earnings_info": "Earnings Data",
        "benefit_info": "Benefits Data",
        "fe_info": "Field Economist Info",
        "comp_info": "Company Info"
        }


output_loc=r"//Documents/"

def change_status():
    global status
    global output_loc

    if status=="dev":
        status="prod"
        output_loc=r"//Documents/"

    elif status=="prod":
        status="dev"
        output_loc=r"//Documents/"




rw_day=datetime.date.today()


def to_sql_date(date):
    return date.strftime("%d-%b-%y").upper() 


def set_date(month, day, year):
    date=datetime.date(year, month, day)
    global rw_day
    rw_day=date

def get_date():
    return rw_day

def day_plus_1(day):
    return day+datetime.timedelta(days=1)

def ws_section(data_df):
    start="<div class=\"section\">"
    data=data_df.to_html(index=False, classes='section', justify='center') # add css class as option. 
    end="</div>"
    section= data+ end
    return section

def ws_build(sched_indv, sched_num):

    head=""" <!DOCTYPE html>
            <html lang="en">
            <head>
            <meta charset="UTF-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title> Worksheet for """+str(sched_num)+"""</title>"""
    
    style="""  <style>
    body {
      font-family: Arial, sans-serif;
      background: #f5f7fa;
      margin: 2em;
      color: #333;
    }

    h1 {
      color: #2c3e50;
      border-bottom: 2px solid #ccc;
      padding-bottom: 0.3em;
    }

    .section h2 {
      color: #34495e;
      margin-top: 0;
    }

    .data-point {
      display: flex;
      justify-content: space-between;
      padding: 0.5em 0;
      border-bottom: 1px solid #eee;
    }

    .data-point:last-child {
      border-bottom: none;
    }

    .label {
      font-weight: bold;
    }

    .value {
      font-family: monospace;
    }

    
    .section {
      margin-bottom: 2em;
      background: #fff;
      padding: 1.5em;
      border-radius: 10px;
      box-shadow: 0 2px 5px rgba(0,0,0,0.05);
      width: 95%;
      

    }
    table{
      overflow: hidden;
      word-break: break-word;
      /* overflow-wrap: normal; */
      table-layout: auto;
    }

    th{
      background-color: darkslategray;
      color: white;
    }




  </style>"""
    script=""
    body=" <h1>Worksheet for Schedule: "+str(sched_num)+ "</h1>"
    body+="<h3> Links: </h3>"
    body+="""<ul>
            <li><a href="https://data.bls.gov/cew/apps/bls_naics/v2/bls_naics_app.htm#tab=search&amp;naics=2017&amp;keyword=%22news%22&amp;searchType=indexes&amp;filter=6_filter&amp;sort=text_asc&amp;resultIndex=0">NAICS
            Industry Finder</a></li>
            <li><a href="https://www.bls.gov/soc/2018/soc_2018_manual.pdf">2018 SOC
            Manual</a></li>
            <li><a href="https://www.onetonline.org/">ONet</a></li>
            <li><a href="http://ofo.cfsp.bls.gov/NCS/Training/Refresher%20Training%20Content/Leveling%20Guide%20revised%206-24-19.pdf#search=leveling%20manual">4
            Factor Leveling Guide</a></li>
            <li><a href="https://edbweb92.bls.gov/EdbWeb/">EDB</a></li>
            <li><a href="https://edbweb92.bls.gov/resources/edb_report.html">EDB Report</a></li>
            </ul>"""
    for data_group in sched_indv:

        if data_group== "rand_hit": 
            body+="<h3> Your random quote is " + str(sched_indv[data_group][0]) + " - " + str(sched_indv[data_group][1]) +"</h3>"
        
        elif data_group=="benefit_info":
            for b_section in sched_indv[data_group]:
                body+="<h2>" + b_section + "</h2>"
                body+= "<div class=\""+data_group+"\"> "+ws_section(sched_indv[data_group][b_section])
        else:
            body+="<h2>" + header[data_group] + "</h2>"
            body+="<div class=\""+data_group+"\"> "+ws_section(sched_indv[data_group])

    tail="</body></html>"
    ws= head+style+script+body+tail
    return ws


def output_ws(un, pw, ben, custom=False, new_fold=None):
    if custom==True:
        if new_fold is not None:
                m_folder=q_mgr_loc+"/"+ new_fold
        else:
            m_folder=q_mgr_loc
    else:
        m_folder=output_loc
    if not os.path.exists(m_folder):
          os.mkdir(m_folder)
    format=format_pull(un, pw, ben, custom=custom)
    for sched in format:
        ws_name=str(sched)+".html"
        if custom==True:
            ws=ws_build(format[sched], str(sched))
            with open(m_folder+"/"+ ws_name, "w") as f:
                f.write(ws)
        else:
            reviewer= format[sched]['admin_info']["RV_FIRSTNAME"].item()+" "+format[sched]['admin_info']["RV_LASTNAME"].item()
            if not os.path.exists(m_folder+"/"+reviewer):
                os.mkdir(m_folder+"/"+reviewer)
            ws=ws_build(format[sched], str(sched))
            with open(m_folder+"/"+reviewer+"/"+ ws_name, "w") as f:
                f.write(ws)





def db_pull(un, pw, select):
    cs = '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = cfdbprd01.psb.bls.gov)(PORT = 1521))    (CONNECT_DATA =      (SERVER = DEDICATED) (SERVICE_NAME = ocwcprd.psb.bls.gov)))'
    # cs stands for connection string. This is specific to the ocwcprd oracle database, but should be the same for all users that have access. 
    pull=[]
    with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
        with connection.cursor() as cursor:
            sql = select
            cursor.execute(sql)
            columns = [col.name for col in cursor.description]
            # pull.append(columns)
            rows=cursor.fetchall()
            for r in rows:
                pull.append([r_i for r_i in r])
    # return pull
    return pd.DataFrame(pull, columns=columns)


    
def format_pull(un, pw, ben, custom=False):
    formatted= {}
    if custom==True:
        a=db_pull(un, pw, get_custom())
    else:
        a=db_pull(un, pw, get_admin())

    f=db_pull(un, pw, get_fe_info())
    c=db_pull(un, pw, get_comp_data())
    r=db_pull(un, pw, get_resp())
    e=db_pull(un, pw, get_earn())
    b=get_all_ben(un, pw, ben) # dictionary of "ben_group name": df of sql pull results

    for s in sched_list:
        num_quotes= e[e["SCHED"]==s]
        # rw_date=int(a.loc[0, "DATE_RW_RUN"].timestamp())
        use_quotes=num_quotes[num_quotes["OCC_STATUS"]=="USE"]["HIT_NUMBER"].to_list()
        random.seed(s)

        if len(use_quotes)==0:
                tnr_check=num_quotes[num_quotes["OCC_STATUS"]=="TNR"]["HIT_NUMBER"].to_list()
                if len(tnr_check)==0:
                    rand_hit=1
                else:
                    rand_hit=random.choice(tnr_check)
        else:
                rand_hit=random.choice(use_quotes)




        rand_hit_title=e[(e["SCHED"]==s) & (e["HIT_NUMBER"]==rand_hit)]["OCC_DESCRIPTION_A"].item()
        ben_sections={}
        for b_sec in b:
            indv_sect=b[b_sec]
            if "HIT_NUMBER" in indv_sect:
                s_filt=indv_sect[(indv_sect["SCHED"]==s)]
                ben_sections[b_sec]=(s_filt[s_filt["HIT_NUMBER"]==rand_hit])
                # ben_sections[b_sec]=indv_sect[(indv_sect["SCHED"]==s)&(indv_sect["HIT_NUMBER"]==rand_hit)]
            else:
                ben_sections[b_sec]=indv_sect[(indv_sect["SCHED"]==s)]
    
              
        if s not in formatted:
            formatted[s]={
                "admin_info": a[a["SCHED"]==s],
                "fe_info": f[f["SCHED"]==s],
                "comp_info": c[c["SCHED"]==s], 
                "resp_info": r[r["SCHED"]==s],
                "earnings_info": e[e["SCHED"]==s],
                "rand_hit": [rand_hit, rand_hit_title],
                "benefit_info": ben_sections
                }
        else: 
            print ("repeating sched in sched list")

    return formatted



def get_rw_summary(un, pw):
    global sched_list
    global cycle
    rw_res=db_pull(un, pw, get_prev())
    if not rw_res.empty:
        cycle= max(rw_res["CYCLE"].to_list())
    else: 
        cycle="000"
    sched_list=rw_res["SCHED"].to_list()
    # rw_res["FE Name"]= str(rw_res["FE_FIRSTNAME"])+" "+ str(rw_res["FE_LASTNAME"])
    return rw_res[['SCHED','CYCLE', 'DATE_RW_RUN', 'REVIEWER_NAME', 
       'REVIEW_TYPE',  'FE_FIRSTNAME', 'FE_LASTNAME',
       'COMPANY_NAME']]

def format_prev(un, pw):
    rw_res= get_rw_summary(un, pw)
    if rw_res.empty:
        final="No schedules found for the current RW date selection, please confirm date."
    else:
        final= rw_res.to_string()
    return final 

def get_custom_summary(un, pw, c_sched, cyc):
    global sched_list
    global cycle
    sched_list=c_sched.split(",")
    cycle= cyc
    if len(c_sched)==0:
        return "No custom schedules were entered, please add schedules and cycle to entry boxes" 
    else:
        rw_res=db_pull(un, pw, get_custom())
        sched_list=rw_res["SCHED"].to_list()
        # rw_res["FE Name"]= str(rw_res["FE_FIRSTNAME"])+" "+ str(rw_res["FE_LASTNAME"])
        return rw_res[['SCHED','CYCLE', 'FE_FIRSTNAME', 'FE_LASTNAME',
        'COMPANY_NAME']]

def get_scheds():
    sql_format="'"+("', '".join(str(x) for x in sched_list))+"'"
    return sql_format

def list_to_sql(pylist):
    return "'"+("', '".join(str(x) for x in pylist))+"'"

    

#####################################################
# SQL QUERIES FOR WORKSHEETS #

def get_prev():
    prev= """SELECT 
            rvw.SCHED,
            rvw.DATE_RW_RUN,
            rvw.REVIEWER_NAME,
            rvw.user_id,
            rvw.cycle,
            rvw.REVIEW_TYPE, 

            usr.FIRST_NAME as FE_FirstName,
            usr.LAST_NAME as FE_LastName,
            usr.PHONE_NO as FE_PhoneNumber,
            usr.REGION_CODE as FE_RegionCode,
            usr.STATUS as FE_Status,
            usr.lan_id as FE_LanID ,
    
            estab.COLLECTION_DATE,
            estab.COLLECTION_METHOD,
            estab.COLLECTION_METHOD_BEN,
            estab.COMPANY_NAME,
            estab.ECI_BENEFIT_STATUS,
            estab.ESTAB_EMPLOYMENT,
            estab.pso_employment,
            estab.NAICS_CODE,
            estab.PAYROLL_REFERENCE_DATE,
            estab.PCT_USABLE_QUOTES,
            estab.REGION,
            estab.REMARK_NUMBER,
            estab.PRODUCT_DESCR,
            estab.CYCLE_PSO


            FROM NCS.review_assign rvw 
            left join NCS.fe_assign fea 
            on rvw.sched=fea.sched and rvw.cycle= fea.cycle
            left join NCS.estab estab
            on rvw.sched=estab.sched and rvw.cycle= estab.cycle
            left JOIN NCS.users usr
                ON usr.USER_ID = fea.USER_ID
            WHERE rvw.date_rw_run >= \'""" + to_sql_date(get_date()) + """\' 
                AND rvw.date_rw_run < \'""" + to_sql_date(day_plus_1(get_date())) + """\' 
                AND rvw.REVIEW_TYPE IN ('TRP', 'PCT') """
    return prev

#admin --> Static for all WS
def get_admin():
    admin= """SELECT 
                rvw.SCHED,
                rvw.DATE_RW_RUN,
                rvw.REVIEWER_NAME,
                usr.FIRST_NAME as RV_FirstName,
                usr.LAST_NAME as RV_LastName,
                rvw.user_id,
                rvw.cycle,
                rvw.REVIEW_TYPE

                FROM NCS.review_assign rvw 
                left JOIN NCS.users usr
                ON usr.USER_ID = rvw.USER_ID
                WHERE rvw.date_rw_run >= \'""" + to_sql_date(get_date()) + """\' 
                    AND rvw.date_rw_run < \'""" + to_sql_date(day_plus_1(get_date())) + """\' 
                    AND rvw.REVIEW_TYPE IN ('TRP', 'PCT') """
    return admin

def get_custom(): 

    custom="""SELECT 
                estab.SCHED,
                estab.cycle,

                usr.FIRST_NAME as FE_FirstName,
                usr.LAST_NAME as FE_LastName,
                usr.PHONE_NO as FE_PhoneNumber,
                usr.REGION_CODE as FE_RegionCode,
                usr.STATUS as FE_Status,
                usr.lan_id as FE_LanID ,

                estab.COLLECTION_DATE,
                estab.COMPANY_NAME,
                estab.REGION


                FROM NCS.estab estab
                left join NCS.fe_assign fea 
                on estab.sched=fea.sched and estab.cycle= fea.cycle
                left JOIN NCS.users usr
                ON usr.USER_ID = fea.USER_ID
        WHERE estab.SCHED  IN (""" +get_scheds()+""") and estab.cycle in("""+str(cycle)+""")"""
    return custom 

def get_fe_info():
    fe="""SELECT 
            fea.SCHED,
            fea.CYCLE,
            usr.FIRST_NAME,
            usr.LAST_NAME,
            usr.PHONE_NO,
            usr.REGION_CODE,
            usr.STATUS,
            usr.lan_id
        FROM NCS.fe_assign fea
        left JOIN NCS.users usr
            ON usr.USER_ID = fea.USER_ID
        WHERE fea.SCHED IN  (""" +get_scheds()+""") 
        and fea.cycle in("""+str(cycle)+""")"""
    return fe

def get_comp_data():
    comp_data="""SELECT 
            estab.SCHED,
            estab.CYCLE,
            estab.COLLECTION_DATE,
            estab.COLLECTION_METHOD,
            estab.COLLECTION_METHOD_BEN,
            estab.COMPANY_NAME,
            estab.ECI_BENEFIT_STATUS,
            estab.ESTAB_EMPLOYMENT,
            estab.pso_employment,
            estab.NAICS_CODE,
            estab.PAYROLL_REFERENCE_DATE,
            estab.PCT_USABLE_QUOTES,
            estab.REGION,
            estab.REMARK_NUMBER,
            estab.PRODUCT_DESCR,
            estab.CYCLE_PSO
        FROM NCS.estab estab
            WHERE estab.SCHED IN  (""" +get_scheds()+""") 
            and estab.cycle in("""+str(cycle)+""")"""
    return comp_data

def get_resp():
    cma_resp="""SELECT s.SCHED,
                r.FULL_NAME,
                r.RESPONDENT_TITLE,
                r.E_MAIL,
                r.PHONE,
                r.CELL_PHONE,
                r.CITY,
                r.STATE,
                r.STREET_ADDRESS,
                b.BIN_ID,
                r.STATUS,
                r.SUPPLYING,
                r.AUTHORIZING,
                r.RESPONDENT_ID,
                s.PL_STREET_ADDRESS,
                s.PL_CITY,
                s.PL_STATE,
                s.PL_COMPANY_NAME,
                s.PL_SECONDARY_NAME
                FROM NCS.cma_schedule s
                INNER JOIN ncs.cma_bin b
                ON s.BIN_ID = b.BIN_ID
                INNER JOIN NCS.cma_respondent r
                ON r.BIN_ID    = s.BIN_ID
            WHERE s.SCHED IN  (""" +get_scheds()+""")
        AND r.STATUS = 'A'"""
    return cma_resp

#earn pull includes SOC def and info-- make hover feature? 
def get_earn():
    earn="""SELECT 
            occ.SCHED,
            occ.CYCLE,
            occ.HIT_NUMBER,
            occ.OCC_STATUS,
            occ.OCC_DESCRIPTION_A,
            occ.SOC_2018,
            soc.SOC_TITLE,
            soc.SOC_DEFINITION,

            occ.HOURS_PER_DAY,
            occ.HOURS_PER_WEEK,
            occ.weeks_per_year,
            occ.FULL_PART_TIME,
            occ.TIME_INCENTIVE,
            occ.UNION_NONUNION,
            occ.IVF_GRADE_LEVEL,
            occ.occ_employment,
            occ.AVG_HRLY_RATE,
            occ.OCC_PERCENT_CHANGE,
            occ.PAY_RELATIVE,
            occ.PERCENT_SPREAD
          
        FROM NCS.OCC occ
        left JOIN NCS.soc_definitions soc
        ON soc.SOC_CODE  = occ.SOC_CODE
        WHERE occ.SCHED IN  (""" +get_scheds()+""")
                AND occ.CYCLE IN ("""+str(cycle)+""")"""
    return earn


#Quarterly Benefit Options

# health=["11"]
def get_health():
    health={
          "Health Insurance Provisions":
                          """SELECT NCS.OCC.SCHED,
                            NCS.OCC.HIT_NUMBER,
                            NCS.PLAN_TABLE.LINKAGE_ID,
                            NCS.PLAN_TABLE.PLAN_ID,
                            NCS.PLAN_TABLE.PLAN_NAME,
                            NCS.PLAN_TABLE.PLAN_TYPE,
                            NCS.HEALTH_PROV.INS_TYPE_MEDICAL,
                            NCS.HEALTH_PROV.INS_TYPE_DENTAL,
                            NCS.HEALTH_PROV.INS_TYPE_VISION,
                            NCS.HEALTH_PROV.INS_TYPE_DRUGS,
                            NCS.HEALTH_PROV.INS_TYPE_ND,
                            NCS.HEALTH_PROV.CYCLE_BEGIN,
                            NCS.HEALTH_PROV.CYCLE_END,
                            NCS.HEALTH_PROV.SELF_INSURED_PLAN,
                            NCS.HEALTH_PROV.THIRD_PARTY_ADMIN,
                            NCS.PLAN_TABLE.ELIGIBILITY_DAYS,
                            NCS.HEALTH_PROV.CATASTROPHIC_INSUR_PROTECT,
                            NCS.HEALTH_PROV.PREPAID_PROV,
                            NCS.HEALTH_PROV.PLAN_PROVIDER_RESTRICTED,
                            NCS.HEALTH_PROV.POINT_OF_SERVICE_OPTION,
                            NCS.HEALTH_PROV.ER_CONVERSION_CODE,
                            NCS.HEALTH_PROV.EE_CONVERSION_CODE
                        FROM NCS.ESTAB,
                            NCS.OCC,
                            NCS.OCC_LINK,
                            NCS.PLAN_TABLE,
                            NCS.HEALTH_PROV
                        WHERE (NCS.ESTAB.CYCLE      = NCS.OCC.CYCLE)
                            AND (NCS.ESTAB.SCHED          = NCS.OCC.SCHED)
                            AND (NCS.OCC.SCHED            = NCS.OCC_LINK.SCHED)
                            AND (NCS.OCC.HIT_NUMBER       = NCS.OCC_LINK.HIT_NUMBER)
                            AND (NCS.OCC_LINK.BEN_NUM     = NCS.PLAN_TABLE.BEN_NUM)
                            AND (NCS.OCC_LINK.SCHED       = NCS.PLAN_TABLE.SCHED)
                            AND (NCS.OCC_LINK.LINKAGE_ID  = NCS.PLAN_TABLE.LINKAGE_ID)
                            AND (NCS.OCC_LINK.SCHED       = NCS.HEALTH_PROV.SCHED)
                            AND (NCS.OCC_LINK.BEN_NUM     = NCS.HEALTH_PROV.BEN_NUM)
                            AND (NCS.PLAN_TABLE.PLAN_ID   = NCS.HEALTH_PROV.PLAN_ID)
                            AND (NCS.PLAN_TABLE.CYCLE_END = NCS.HEALTH_PROV.CYCLE_END)
                            AND NCS.PLAN_TABLE.BEN_NUM    = 11
                            AND NCS.PLAN_TABLE.CYCLE_END  = 99999
                            AND NCS.ESTAB.SCHED           IN ("""+get_scheds()+""") 
                            AND NCS.ESTAB.CYCLE     IN ("""+str(cycle)+""")""", 

      "Health Insurance Premiums":
                              """SELECT NCS.health_prov_premium.SCHED
                              , NCS.health_prov_premium.COVERAGE_OPTION
                              , NCS.health_prov_premium.ITEM_NUMBER
                              , NCS.health_prov_premium.ER_PREMIUM
                              , NCS.health_prov_premium.EE_PREMIUM
                              , NCS.health_prov_premium.CYCLE_BEGIN
                              ,   NCS.health_prov_premium.PLAN_ID
                          FROM NCS.health_prov_premium
                          WHERE NCS.health_prov_premium.CYCLE_END = 99999
                          AND NCS.health_prov_premium.SCHED IN ("""+get_scheds()+""")""", 
      "Health Insurance Costs":
                            """SELECT ncs.estab.sched,
                                ncs.occ.hit_number,
                                ncs.cost.linkage_id,
                                ncs.cost.cost_id,
                                ncs.cost.cost_name,
                                ncs.cost.cost_type,
                                ncs.health.ben_stat,
                                ncs.health.source,
                                ncs.health.value_entry,
                                ncs.health.conversion_code,
                                ncs.health.cycle_begin,
                                ncs.health.cycle_end,
                                ncs.occ.occ_description_a
                            FROM  ncs.cost,
                                ncs.estab,
                                ncs.occ,
                                ncs.health
                            WHERE  (ncs.estab.sched   = ncs.occ.sched)
                                and (ncs.estab.CYCLE  = ncs.occ.CYCLE)
                                and (ncs.estab.sched     = ncs.cost.sched)
                                and (ncs.occ.sched       = ncs.health.sched)
                                and (ncs.occ.hit_number  = ncs.health.hit_number)
                                and (ncs.cost.ben_num    = ncs.health.ben_num)
                                and (ncs.cost.cost_id    = ncs.health.cost_id)
                                and ncs.health.cycle_end = 99999
                                and ncs.cost.cycle_end   = 99999 
                                AND NCS.ESTAB.SCHED           IN ("""+get_scheds()+""") 
                                AND NCS.ESTAB.CYCLE     IN ("""+str(cycle)+""")"""

                        } 
    return health  
# leave=["02", "03", "04", "05"] 
def get_leave():
    
    leave={
        "Leave Overview":
                            """SELECT NCS.benefit_hit.SCHED,
                            NCS.benefit_hit.BEN_NUM,
                            NCS.benefit_hit.HIT_NUMBER,
                            NCS.benefit_hit.BEN_HIT_STATUS_OVERVIEW,
                            NCS.benefit_hit.CYCLE_MATERIAL_CHANGE,
                            NCS.benefit_hit.ANNUAL_COST,
                            NCS.benefit_hit.PERCENT_CHANGE,
                            NCS.benefit_hit.PERCENT_STR,
                            NCS.benefit_hit.ANN_PERCENT_CHANGE
                            FROM NCS.benefit_hit
                            WHERE NCS.benefit_hit.SCHED  IN ("""+get_scheds()+""") 
                            AND NCS.benefit_hit.BEN_NUM  IN (2, 3, 4, 5)
                            AND NCS.benefit_hit.CYCLE_END = 99999""", 

        "Vacation: Cost":
                        """SELECT vac.SCHED,
                            vac.COST_ID,
                            vac.HIT_NUMBER, 
                            vac.BEN_STAT,
                            vac.SOURCE,
                            vac.VALUE_ENTRY,
                            vac.CONVERSION_CODE,
                            vac.PAID_WEEKS,
                            vac.UNPAID_WEEKS,
                            vac.ALT_WORK_SCHED,
                            vac.SERVICE_PLAN_NUM,
                            vac.SET_NUMBER,
                            vac.CYCLE_BEGIN,
                            vac.CYCLE_END
                            FROM NCS.vacation vac
                            WHERE vac.SCHED IN ("""+get_scheds()+""") 
                            AND cycle_end = 99999""", 

        "Vacation: Provisions":
                        """SELECT vp.SCHED,
                            vp.BEN_NUM,
                            vp.PLAN_ID,
                            vp.CONSOLIDATED_LEAVE_PLAN,
                            vp.CLP_TYPE_ND,
                            vp.CYCLE_END,
                            vp.SERVICE_PLAN_ND,
                            vp.SERVICE_PLAN_NUM
                        FROM NCS.vac_prov vp
                        WHERE vp.SCHED IN ("""+get_scheds()+""") 
                        AND cycle_end = 99999""",

        "Vacation: Service Plans":
                    """
                    SELECT  sp.sched
                    , sp.ben_num
                    , sp.service_plan_num
                    , sp.item_number
                    , sp.weeks_entered
                    , sp.days_entered
                    , sp.work_hours
                    , sp.accrued_hours
                    , sp.los_months_computed
                FROM    ncs.service_plan_detail sp
                WHERE   sp.sched IN ("""+get_scheds()+""") 
                    AND   sp.cycle_end = 99999""", 

        "Holiday: Cost" :

                    """SELECT hd.SCHED,
                        hd.BEN_NUM,
                        hd.COST_ID,
                        hd.HIT_NUMBER,
                        hd.BEN_STAT,
                        hd.SOURCE,
                        hd.VALUE_ENTRY,
                        hd.CONVERSION_CODE,
                        hd.PAID_DAYS,
                        hd.UNPAID_DAYS,
                        hd.ALT_WORK_SCHED,
                        hd.CYCLE_END
                    FROM NCS.holidays hd
                    WHERE hd.SCHED IN ("""+get_scheds()+""") 
                    AND hd.cycle_end = 99999""",

        "Sick Leave: Provisions":
                    """
                    SELECT slp.SCHED,
                    slp.CYCLE_BEGIN,
                    slp.PLAN_ID,
                    slp.BEN_NUM,
                    slp.SL_GRANTED,
                    slp.SERVICE_PLAN_NUM,
                    slp.CARRY_OVER_ALLOWED,
                    slp.TYPE_CARRY_OVER,
                    slp.MAXIMUM_ACCUMULATION,
                    slp.MAXIMUM_ACCUMULATION_DAYS,
                    slp.MAXIMUM_ACCUMULATION_HOURS
                    FROM NCS.sick_leave_prov slp
                    WHERE slp.CYCLE_END = 99999
                    AND slp.SCHED      IN ("""+get_scheds()+""")""" , 

        "Sick Leave: Cost":
                    """
                    SELECT sk.SCHED,
                    sk.BEN_NUM,
                    sk.COST_ID,
                    sk.HIT_NUMBER,
                    sk.BEN_STAT,
                    sk.SOURCE,
                    sk.VALUE_ENTRY,
                    sk.CONVERSION_CODE,
                    sk.PAID_DAYS,
                    sk.UNPAID_DAYS,
                    sk.COST_CODE
                    FROM NCS.sick_leave sk
                    WHERE sk.SCHED IN ("""+get_scheds()+""")  
                    AND   sk.cycle_end = 99999""",

        "Other/Personal Leave":
                """
                SELECT ol.SCHED,
                    ol.COST_ID,
                    ol.HIT_NUMBER,
                    ol.BEN_STAT,
                    ol.SOURCE,
                    ol.VALUE_ENTRY,
                    ol.CONVERSION_CODE,
                    ol.PAID_DAYS,
                    ol.UNPAID_DAYS,
                    ol.ALT_WORK_SCHED
                FROM NCS.other_leave ol
                WHERE ol.cycle_end = 99999
                AND ol.sched IN ("""+get_scheds()+""")""",


        "Leave: Emergent Benefits": 
                """SELECT DISTINCT
                eb.SCHED,
                eb.BEN_NUM,
                eb.CYCLE_END,
                eb.HIT_NUMBER,
                eb.ELIGIBLITY,
                bt.BEN_TITLE
                FROM NCS.emerg_ben eb
                JOIN ncs.ben_title bt
                ON bt.BEN_NUM      = eb.BEN_NUM
                WHERE eb.SCHED     IN ("""+get_scheds()+""") 
                AND eb.BEN_NUM     IN (83, 84, 85, 86, 87, 88)
                AND eb.ELIGIBLITY IS NOT NULL"""
               
        }
    return leave
# retirement=["13", "14"]
def get_retirement():
    retirement={
                
    "Retirement Overview": """
                        SELECT NCS.benefit_hit.SCHED,
                            NCS.benefit_hit.BEN_NUM,
                            NCS.benefit_hit.HIT_NUMBER,
                            NCS.benefit_hit.BEN_HIT_STATUS_OVERVIEW,
                            NCS.benefit_hit.CYCLE_MATERIAL_CHANGE,
                            NCS.benefit_hit.ANNUAL_COST,
                            NCS.benefit_hit.PERCENT_CHANGE,
                            NCS.benefit_hit.PERCENT_STR,
                            NCS.benefit_hit.ANN_PERCENT_CHANGE
                        FROM NCS.benefit_hit
                        WHERE NCS.benefit_hit.SCHED   IN ("""+get_scheds()+""") 
                        AND NCS.benefit_hit.BEN_NUM  IN (13, 14)
                        AND NCS.benefit_hit.CYCLE_END = 99999
                                """, 
                    

    "Defined Benefit Cost":

                            """
                            SELECT dbc.COST_ID,
                                dbc.HIT_NUMBER,
                                dbc.SCHED,
                                dbc.BEN_STAT,
                                dbc.SOURCE,
                                dbc.VALUE_ENTRY,
                                dbc.CONVERSION_CODE,
                                dbc.CEILING,
                                dbc.ALT_WORK_SCHED
                            FROM NCS.defined_ben dbc
                            WHERE dbc.cycle_end = 99999
                            AND dbc.sched  IN ("""+get_scheds()+""")""", 

    "Defined Contribution Cost":

                                """SELECT dcc.SCHED,
                                    dcc.BEN_NUM,
                                    dcc.COST_ID,
                                    dcc.HIT_NUMBER,
                                    dcc.BEN_STAT,
                                    dcc.SOURCE,
                                    dcc.VALUE_ENTRY,
                                    dcc.CONVERSION_CODE,
                                    dcc.CEILING,
                                    dcc.ALT_WORK_SCHED,
                                    dcc.CYCLE_BEGIN,
                                    dcc.EE_SAVINGS_PCT
                                FROM NCS.defined_contr dcc
                                WHERE dcc.CYCLE_END = 99999
                                AND dcc.SCHED       IN ("""+get_scheds()+""") 
                                        """, 
    "Defined Contribution Provisions": 
                                """SELECT dcp.SCHED,
                                    dcp.BEN_NUM,
                                    dcp.PLAN_ID,
                                    dcp.DCP_TYPE,
                                    dcp.EMPLOYEE_CONTRIB_REQUIRED,
                                    dcp.EMPLOYEE_CONTRIB_TAX_DEF
                                FROM NCS.def_con_prov dcp
                                WHERE dcp.CYCLE_END = 99999
                                AND dcp.SCHED IN ("""+get_scheds()+""") 
                                        """,



    "Participation":

                        """
                        SELECT p.BEN_NUM,
                        p.SCHED,
                        p.PLAN_ID,
                        p.HIT_NUMBER,
                        p.AVAILABILITY,
                        p.PCT_PARTIC,
                        p.CYCLE_BEGIN
                        FROM NCS.participation p
                        WHERE p.BEN_NUM IN (13, 14)
                        AND p.CYCLE_END  = 99999
                        AND p.sched IN ("""+get_scheds()+""") 
                                """, 


    "Retirement: Emergent Benefits":

                            """
                            SELECT DISTINCT
                            eb.SCHED,
                            eb.BEN_NUM,
                            eb.CYCLE_END,
                            eb.HIT_NUMBER,
                            eb.ELIGIBLITY,
                            bt.BEN_TITLE
                            FROM NCS.emerg_ben eb
                            JOIN ncs.ben_title bt
                            ON bt.BEN_NUM      = eb.BEN_NUM
                            WHERE eb.SCHED     IN ("""+get_scheds()+""")
                            AND eb.BEN_NUM     IN (49, 51, 54, 55, 59, 76)
                            AND eb.cycle_end = 99999
                            AND eb.ELIGIBLITY IS NOT NULL
                            """ 
    }
    return retirement
# life_disab=["10", "12", "23"] #10=Life ; #12 = STD; #23= LTD
def get_life_disab():
    life={}
    codes= {
        '10': "Life Insurance (LIFE)", 
        '12':  "Short-term Disability Insurance (STD)", 
        '23': "Long-term Disability Insurance (LTD)"
    }

    for code in codes:
        life[codes[code]]=  """SELECT NCS.benefit_hit.SCHED,
                            NCS.benefit_hit.BEN_NUM,
                            NCS.benefit_hit.HIT_NUMBER,
                            NCS.benefit_hit.BEN_HIT_STATUS_OVERVIEW,
                            NCS.benefit_hit.CYCLE_MATERIAL_CHANGE,
                            NCS.benefit_hit.ANNUAL_COST,
                            NCS.benefit_hit.PERCENT_CHANGE,
                            NCS.benefit_hit.PERCENT_STR,
                            NCS.benefit_hit.ANN_PERCENT_CHANGE
                            FROM NCS.benefit_hit
                            WHERE NCS.benefit_hit.SCHED  IN ("""+get_scheds()+""")  
                            AND NCS.benefit_hit.BEN_NUM  IN (""" +code+""")
                            AND NCS.benefit_hit.CYCLE_END = 99999"""
    return life

# supplemental=["01", "06", "07", "15", "16", "19", "20", "21"]
def get_supplemental():
    supp={}
    codes= {
        '1':  "Overtime (OT)",
        '06': "Shift Differential (SHIFT)",
        '07': "Nonproduction Bonus (NPB)",
        '15': "Social Security (SS)",
        '16':  "Medicare (MED)",
        '19': "Federal Unemployment Tax Act (FUTA)",
        '20':  "State Unemployment Insurance (SUI)",
        '21':  "Worker's Compensation (WC)"
    }

    for code in codes:
        supp[codes[code]]=  """SELECT NCS.benefit_hit.SCHED,
                            NCS.benefit_hit.BEN_NUM,
                            NCS.benefit_hit.HIT_NUMBER,
                            NCS.benefit_hit.BEN_HIT_STATUS_OVERVIEW,
                            NCS.benefit_hit.CYCLE_MATERIAL_CHANGE,
                            NCS.benefit_hit.ANNUAL_COST,
                            NCS.benefit_hit.PERCENT_CHANGE,
                            NCS.benefit_hit.PERCENT_STR,
                            NCS.benefit_hit.ANN_PERCENT_CHANGE
                            FROM NCS.benefit_hit
                            WHERE NCS.benefit_hit.SCHED  IN ("""+get_scheds()+""")  
                            AND NCS.benefit_hit.BEN_NUM  IN (""" +code+""")
                            AND NCS.benefit_hit.CYCLE_END = 99999"""
    return supp



def get_all_ben(un, pw, t_benefit):
    ui_opt_map={

        "Health Insurance":get_health(), 
        "Leave": get_leave(), 
        "Retirement": get_retirement(), 
        "Life & Disability": get_life_disab(), 
        "Supplemental/Legally Required": get_supplemental()
    }
    if type(t_benefit) is list: #for multiple benifit selection
        b={}
        for ben in t_benefit:
            b.update(ui_opt_map[ben])
    else:
        b=ui_opt_map[t_benefit]
    # b=get_retirement()
    res={}
    for ben_group in b: 
        res[ben_group]=db_pull(un, pw, b[ben_group])
    return res

    #returns all ben groups as dictionary of ben group title and resulting df of pull




###############################################
