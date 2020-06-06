
select a.* 
from ingredients a 
where a.time_stamp in(select max(time_stamp) from ingredients  where deleted=0  group by id_ingredient, ingredient_name)
order by a.id_ingredient,a.ingredient_name;


Sample Output:

 id_ingredient	ingredient_name		price	time_stamp	deleted
1	1				potatoes		10		1445899655	0
2	1				sweet potatoes	15		1445959231	0
3	2				tomatoes		20		1445836421	0
4	3				chicken			50		1445899655	0
5	999				garlic			17		1445897351	0