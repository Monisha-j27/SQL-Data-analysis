---team has won the maximum gold medals over the years.

select top 1 a.team,count(medal) as total_gold_medals
from athletes a
left join athlete_events ae on ae.athlete_id = a.id and ae.medal != 'NA' 
where medal = 'Gold'
group by a.team
order by total_gold_medals desc

--2 for each team print total silver medals and year in which they won maximum silver medal..output 3 columns

with tsm_cte as
(select a.team,ae.year,count(medal) as total_silver_medals,
Rank() over(partition by team order by count(medal)  desc) as rn
from athletes a
left join athlete_events ae on ae.athlete_id = a.id and ae.medal != 'NA' 
where medal = 'Silver'
group by a.team,ae.year)

select team,sum(total_silver_medals) as total_medal_won,max(case when rn =1 then year end) as max_medal_year
from tsm_cte t
group by team


-- won only gold medal (never won silver or bronze) over the years

with nm_cte as
(select a.name,ae.medal,
count( medal) as medal_count
--Rank() over(partition by name order by ae.medal) as rn
from athletes a
left join athlete_events ae on a.id = ae.athlete_id and medal!='NA' 
where medal is not null
group by a.name,ae.medal)

select top 1 *
from nm_cte
where name not in ( select distinct name from nm_cte where medal in ('silver','Bronze') )
and medal ='Gold'
order by medal_count desc



--no of golds won in that year . In case of a tie print comma separated player names.

with gt_cte as
(select a.name,ae.year,count(medal) as total_gold_medals,
ROW_NUMBER() over(partition by year order by count(medal) desc) as rn
from athletes a
left join athlete_events ae on ae.athlete_id = a.id and ae.medal != 'NA' 
where medal = 'Gold'
group by a.name,ae.year
),

rns_cte as(select * 
from gt_cte
where rn = 1)

select string_agg(g.name,','),g.year,g.total_gold_medals
from gt_cte g
inner join rns_cte r on r.year=g.year and r.total_gold_medals = g.total_gold_medals
group by g.year,g.total_gold_medals


--in which event and year India has won its first gold medal,first silver medal and first bronze medal
--print 3 columns medal,year,sport

with medal_cte as
(select ae.event,ae.year,ae.medal,count(medal) as medal_count
from athletes a
left join athlete_events ae on ae.athlete_id = a.id and ae.medal != 'NA' 
where team = 'india'
group by ae.medal,ae.year,ae.event),

rns as(select * ,ROW_NUMBER() over(partition by medal order by year) as rn
from medal_cte
where medal is not null)

select medal,year,event as sport
from rns 
where rn = 1


--players who won gold medal in summer and winter olympics both

with sos_cte as
(select  a.name,ae.season,ae.medal
from athletes a
left join athlete_events ae on ae.athlete_id = a.id and ae.medal = 'Gold' 
where season in ('Summer','Winter')),

winter_player as(select * from sos_cte where season = 'winter'),

summer_player as(select * from sos_cte where season = 'Summer')

select s.name
from winter_player w
inner join summer_player s on w.name=s.name


--who won gold, silver and bronze medal in a single olympics. print player name along with year

with nmy_cte as(select a.name,ae.medal,ae.year
from athletes a
left join athlete_events ae on ae.athlete_id = a.id and ae.medal != 'NA' 
where medal is not null
group by a.name,ae.year,ae.medal),

gold_cte as (select * from nmy_cte where medal = 'gold'),

silver_cte as (select * from nmy_cte where medal = 'silver'),

Bronze_cte as (select * from nmy_cte where medal = 'Bronze')
select g.name,g.year
from gold_cte g
inner join silver_cte s on g.name=s.name and g.year = s.year
inner join Bronze_cte br on br.name = s.name and br.year = s.year


-8 find players who have won gold medals in consecutive 3 summer olympics in the same event . Consider only olympics 2000 onwards. 
--Assume summer olympics happens every 4 year starting 2000. print player name and event name.
with cte as (
select name,year,event
from athlete_events ae
inner join athletes a on ae.athlete_id=a.id
where year >=2000 and season='Summer'and medal = 'Gold'
group by name,year,event)
select * from
(select *, lag(year,1) over(partition by name,event order by year ) as prev_year
, lead(year,1) over(partition by name,event order by year ) as next_year
from cte) A
where year=prev_year+4 and year=next_year-4
