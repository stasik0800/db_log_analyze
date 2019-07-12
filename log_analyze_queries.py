
def query_q3():
    return  """
    select Count(success_connections),info 
    from (
            select distinct sub.info,df.stmt as success_connections
            from df
            join ( select * from df where info like '%connection id%') as sub on sub.stmt =df.stmt
            where df.info like '%success%'
    ) tb
    group by 2 order by 1 desc;
    """


def query_q5():
    return  """
            select time(last_date_stmt),info 
            from (
                    select max(dt_stmt) as last_date_stmt,trim(stmt)  as stmt
                    from df  
                    group by 2
            ) as source
            join (
                    select distinct trim(stmt) as stmt, info
                    from df 
                    where length(info) <= length('connection id - 000000')
                      and (info like 'connection id -%'  and  not info  like '%executing%' )
                    group by 2
            ) tgt on tgt.stmt = source.stmt;
    """


def query_q7():
    return """
            select stmt,max(dt_stmt_max-dt_stmt_min) 
            from (
                    select stmt,
                       strftime("%f", max(dt_stmt) ) as dt_stmt_max ,
                       strftime("%f ", min(dt_stmt) ) as dt_stmt_min 
                    from df 
                    where stmt in (select stmt from df where info ='success') 
                    group by 1
            )tb;
    """

def query_q8():
    return """
            select count(*) from df;
     
    """

def query_q9():
    return """ 
            select usr,count(stmt) 
            from  (select distinct lower(usr) as usr,trim(stmt) as stmt from df) sub 
            group by 1 
            order by 2 asc;
    """



