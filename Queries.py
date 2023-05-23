import psycopg2

def execute_query(query):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="Anfeidrol2468"
    )

    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    for row in results:
        print(row)

    print()
    
    cursor.close()
    conn.close()

''' 
    (Query 1) Gênero dos leads
    Colunas: gênero, leads(#)
'''
query1 = """
select
	case
		when ibge.gender = 'male' then 'Homens'
		when ibge.gender = 'female' then 'Mulheres'
	end as "Gênero",
	count(*) as "Leads (#)"
from sales.customers as cus
left join temp_tables.ibge_genders as ibge
	on lower(cus.first_name) = lower(ibge.first_name)
group by ibge.gender
"""

'''
    (Query 2) Status profissional dos leads
    Colunas: status profissional, leads (%)
'''
query2 = """
select
	case
		when professional_status = 'freelancer' then 'freelancer'
		when professional_status = 'retired' then 'aposentado(a)'
		when professional_status = 'clt' then 'clt'
		when professional_status = 'self_employed' then 'autônomo(a)'		
		when professional_status = 'other' then 'outro'
		when professional_status = 'businessman' then 'empresário(a)'
		when professional_status = 'civil_servant' then 'funcionário(a) público(a)'
		when professional_status = 'student' then 'estudante'
	end as "Status profissional",
	(count(*)::float)/(select count(*) from sales.customers) as "Leads (%)"
from sales.customers
group by professional_status
order by "Leads (%)"
"""

'''
    (Query 3) Faixa etária dos leads
    Colunas: faixa etária, leads (%)
'''
query3 = """
select
	case
		when datediff('years', birth_date, current_date) < 20 then '0-20'
		when datediff('years', birth_date, current_date) < 40 then '20-40'
		when datediff('years', birth_date, current_date) < 60 then '40-60'
		when datediff('years', birth_date, current_date) < 80 then '60-80'
		else '80+' 
	end "Faixa etária",
	count(*)::float/(select count(*) from sales.customers) as "Leads (%)"
from sales.customers
group by "Faixa etária"
order by "Faixa etária" desc
"""

'''
    (Query 4) Faixa salarial dos leads
    Colunas: faixa salarial, leads (%), ordem
'''
query4 = """
select
	case
		when income < 5000 then '0-5000'
		when income < 10000 then '5000-10000'
		when income < 15000 then '10000-15000'
		when income < 20000 then '15000-20000'
		else '20000+' 
	end "Faixa salarial",
		count(*)::float/(select count(*) from sales.customers) as "Leads (%)",
	case
		when income < 5000 then 1
		when income < 10000 then 2
		when income < 15000 then 3
		when income < 20000 then 4
		else 5 
	end "Ordem"
from sales.customers
group by "Faixa salarial", "Ordem"
order by "Ordem" desc
"""

'''
    (Query 5) Classificação dos veículos visitados
    Colunas: classificação do veículo, veículos visitados (#)
    Regra de negócio: Veículos novos tem até 2 anos e seminovos acima de 2 anos
'''
query5 = """
with
	classificacao_veiculos as (
		select
			fun.visit_page_date,
			pro.model_year,
			extract('year' from visit_page_date) - pro.model_year::int as idade_veiculo,
			case
				when (extract('year' from visit_page_date) - pro.model_year::int)<=2 then 'novo'
				else 'seminovo'
			end as "Classificação do veículo"
		from sales.funnel as fun
		left join sales.products as pro
			on fun.product_id = pro.product_id	
	)

select
	"Classificação do veículo",
	count(*) as "Veículos visitados (#)"
from classificacao_veiculos
group by "Classificação do veículo"
"""

'''
(Query 6) Idade dos veículos visitados
Colunas: Idade do veículo, veículos visitados (%), ordem
'''
query6 = """
with
	faixa_de_idade_dos_veiculos as (
		select
			fun.visit_page_date,
			pro.model_year,
			extract('year' from visit_page_date) - pro.model_year::int as idade_veiculo,
			case
				when (extract('year' from visit_page_date) - pro.model_year::int)<=2 then 'até 2 anos'
				when (extract('year' from visit_page_date) - pro.model_year::int)<=4 then 'de 2 à 4 anos'
				when (extract('year' from visit_page_date) - pro.model_year::int)<=6 then 'de 4 à 6 anos'
				when (extract('year' from visit_page_date) - pro.model_year::int)<=8 then 'de 6 à 8 anos'
				when (extract('year' from visit_page_date) - pro.model_year::int)<=10 then 'de 8 à 10 anos'
				else 'acima de 10 anos'
			end as "Idade do veículo",
			case
				when (extract('year' from visit_page_date) - pro.model_year::int)<=2 then 1
				when (extract('year' from visit_page_date) - pro.model_year::int)<=4 then 2
				when (extract('year' from visit_page_date) - pro.model_year::int)<=6 then 3
				when (extract('year' from visit_page_date) - pro.model_year::int)<=8 then 4
				when (extract('year' from visit_page_date) - pro.model_year::int)<=10 then 5
				else 6
			end as "Ordem"
		from sales.funnel as fun
		left join sales.products as pro
			on fun.product_id = pro.product_id	
	)

select
	"Idade do veículo",
	count(*)::float/(select count(*) from sales.funnel) as "Veículos visitados (%)",
	"Ordem"
from faixa_de_idade_dos_veiculos
group by "Idade do veículo", "Ordem"
order by "Ordem"
"""

'''
(Query 7) Veículos mais visitados por marca
Colunas: brand, model, visitas (#)
'''
query7 = """
select
	pro.brand as "Marca",
	pro.model as "Modelo",
	count(*) as "Visitas (#)"
from sales.funnel as fun
left join sales.products as pro
	on fun.product_id = pro.product_id
group by "Marca", "Modelo"
order by "Marca", "Modelo", "Visitas (#)"
"""
execute_query(query1)
execute_query(query2)
execute_query(query3)
execute_query(query4)
execute_query(query5)
execute_query(query6)
execute_query(query7)