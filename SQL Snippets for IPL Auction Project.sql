-- database created for the analysis
create database ipl_auction_analysis;


-- created table ipl_ball before importing table from csv file
create table ipl_ball (
	id INT,
    inning INT,
    over INT,
    ball INT,
    batsman VARCHAR(255),
    non_striker VARCHAR(255),
    bowler VARCHAR(255),
    batsman_runs INT,
    extra_runs INT,
    total_runs INT,
    is_wicket INT,
    dismissal_kind VARCHAR(255),
    player_dismissed VARCHAR(255),
    fielder VARCHAR(255),
    extras_type VARCHAR(255),
    batting_team VARCHAR(255),
    bowling_team VARCHAR(255)
);


-- imported table ipl_ball from csv file
copy ipl_ball 
	from 'C:\Program Files\PostgreSQL\16\pgAdmin 4\IPL Dataset\IPL_Ball.csv' delimiter ',' csv header;


-- created table ipl_matches before importing table from csv file
CREATE TABLE ipl_matches (
    id INT,
    city VARCHAR(255),
    match_date DATE,
    player_of_match VARCHAR(255),
    venue VARCHAR(255),
    neutral_venue INT,
    team1 VARCHAR(255),
    team2 VARCHAR(255),
    toss_winner VARCHAR(255),
    toss_decision VARCHAR(50),
    winner VARCHAR(255),
    result VARCHAR(50),
    result_margin VARCHAR(255),
    eliminator VARCHAR(50),
    method VARCHAR(50),
    umpire1 VARCHAR(255),
    umpire2 VARCHAR(255)
);

--imported ipl_matches csv file
copy ipl_matches 
	from 'C:\Program Files\PostgreSQL\16\pgAdmin 4\IPL Dataset\IPL_matches.csv' delimiter ',' csv header;


-- created a new table for ipl_ball table with year specified exracted from ipl_matches match_date
-- to define number of IPL seasons we are using year from match date which will denote IPL season year
create table ipl_ball_years as 
	select a.*,(Extract(year from b.match_date)) as ipl_year from ipl_ball 
	as a left join ipl_matches as b 
	on a.id = b.id;


-- top 10 agressive batsman with highest strike rate and played more than 500 balls
-- also added the column with number of season they played to have a better insight with respect to strike rate
select 
	batsman, 
	sum(batsman_runs) as total_run, 
	count(ball) as total_balls, 
	round((CAST(sum(batsman_runs) as decimal) /count(ball) *100)) as strike_rate, 
	count(distinct(ipl_year)) as num_of_seasons_played
	from ipl_ball_years group by batsman having count(ball) >=500 order by strike_rate desc limit 10;


-- top 10 anchor batsman with highest average and played in more than 2 IPL seasons
select 
    batsman, 
    total_runs,
    total_dismissals,
    num_of_seasons_played,
    round((cast(total_runs as decimal)/total_dismissals * 100)) as batsman_average
from (
	-- sub query is used aggregrate total runs, dismissals and seasons played by batsman
    select 
     	batsman, 
		sum(batsman_runs) as total_runs, 
		count(is_wicket) as total_dismissals,
		count(distinct(ipl_year)) as num_of_seasons_played
        from ipl_ball_years group by batsman having count(is_wicket) > 0 and count(distinct(ipl_year)) > 2
    ) as subquery order by batsman_average desc limit 10;


-- top 10 hard hitter with highest boundary_percentage and played more than 2 IPL seasons
select 
	batsman,
	num_of_seasons_played, 
	num_of_fours, num_of_sixes, 
	(num_of_fours + num_of_sixes) AS total_boundaries,
	(num_of_fours*4+num_of_sixes*6) as boundary_runs, total_runs,
	round(cast((num_of_fours*4+num_of_sixes*6)as Decimal)/total_runs *100) as boundary_percentage
from (
	-- sub query is used to aggregrate the number of fours and sixes by each batsman
	select batsman, 
	sum(batsman_runs) as total_runs, 
	count(distinct(ipl_year)) as num_of_seasons_played, 
	sum(case when batsman_runs = 4 then 1 else 0 end) as num_of_fours,
	sum(case when batsman_runs = 6 then 1 else 0 end) as num_of_sixes
	from ipl_ball_years group by batsman
) 
as subquery_alias where num_of_seasons_played>2 order by boundary_percentage desc limit 10;


-- top 10 economy bowlers who have bolwed minimum of 500 balls
select 
	bowler,
	total_run, 
	total_balls, 
	total_overs, 
	round((cast((total_run) as Decimal)/total_overs),1) as bowlers_economy
from (
	select
		bowler,
		count(ball) as total_balls, 
		count(over)/6 as total_overs,
		sum(total_runs) as total_run
		from ipl_ball_years group by bowler
) as sub_queries where total_balls >= 500 order by bowlers_economy limit 10;


-- top 10 bowlers with best strike rate and bowled at least 500 balls ordered in ascending. 
-- Lower strike_rate mean less balls bowled in porportion to wicket_taken which is most suitable
select 
	bowler,	
	count(ball) as total_balls, 
	sum(is_wicket) as wicket_taken,
	round(cast(count(ball) as decimal)/sum(is_wicket)) as strike_rate
 	from ipl_ball_years group by bowler having count(ball)>500 order by strike_rate limit 10;


-- top 10 all_rounder with good batting strike rate and bowling strike rate
select 
    a.name as player,  
	a.runs as runs_scored,
	a.balls as ball_played,
    b.balls as balls_bowled,
	b.total_wickets as total_wickets_taken,
	round(cast(a.runs as decimal)/a.balls*100) as batsman_strike_rate,
	round(cast(b.balls as decimal)/b.total_wickets) as bowler_strike_rate
from

	-- first sub query is to get name, runs scored and ball played data by batsman
    (select batsman as name, 
	 sum(batsman_runs) as runs, 
	 count(ball) as balls 
	 from ipl_ball_years group by batsman) as a

	-- inner join will combine common name from both batsman and bowler keeping name as reference to match both table
	inner join 

	-- second sub query is to get bowler, ball baolwed and wicket taken by bowler
    (select bowler as name, count(ball) as balls, sum(is_wicket) as total_wickets from ipl_ball_years group by bowler) 
	as b on a.name = b.name where a.balls>300 and b.balls>500 
	order by batsman_strike_rate desc, bowler_strike_rate asc limit 10;


-- top 5 wicketkeeper who have taken the most stump outs taking only fielder and dismissal_kind column
select 
	fielder as wicket_keeper, 
	count(dismissal_kind) as total_stumps 
	from ipl_ball_years where dismissal_kind = 'stumped' group by fielder order by total_stumps desc limit 5;
