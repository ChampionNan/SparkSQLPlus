create or replace view semiJoinView5280095159905134367 as select src as v2, dst as v4 from Graph AS g2 where (dst) in (select (src) from Graph AS g3);
create or replace view semiJoinView1695388721616032619 as select src as v1, dst as v2 from Graph AS g1 where (dst) in (select (v2) from semiJoinView5280095159905134367);
create or replace view semiEnum7558004606430904072 as select v2, v1, v4 from semiJoinView1695388721616032619 join semiJoinView5280095159905134367 using(v2);
create or replace view semiEnum8695321583543632450 as select v1, v4, v2, dst as v6 from semiEnum7558004606430904072, Graph as g3 where g3.src=semiEnum7558004606430904072.v4;
select v1, v2, v4, v6 from semiEnum8695321583543632450;
