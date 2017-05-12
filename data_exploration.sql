select count(distinct a.user)
from
(select user from nodes union all select user from ways) a
where a.user != '-999';

select a.user, count(1)
from
(select user from nodes union all select user from ways) a
where a.user != '-999'
group by user
order by count(1) desc
limit 20;


SELECT value, count(1)
FROM nodes_tags
WHERE key = 'amenity'
GROUP BY value
HAVING count(1) >= 15
ORDER BY count(1) DESC;

select key, value, count(1)
from ways_tags
where value = 'residential'
group by key,value
order by count(1) DESC
limit 15;


select count(1)
from nodes_tags a, nodes b, ways_tags c, ways_nodes d, ways e
where a.id = b.id
and c.id =e.id
and a.id = d.node_id
and d.id =e.id
and a.key = c.key
and a.value = c.value

select a.key,count(c.key)
from nodes_tags a, nodes b, ways_tags c, ways_nodes d, ways e
where a.id = b.id
and c.id =e.id
and a.id = d.node_id
and d.id =e.id
and a.key = c.key
and a.value = c.value
group by a.key

SELECT value, count(1)
FROM nodes_tags
WHERE key = 'cuisine'
GROUP BY value
ORDER by count(1) DESC

select key,value
from nodes_tags
where value like '%Shop%'
or value like '%diner%'
