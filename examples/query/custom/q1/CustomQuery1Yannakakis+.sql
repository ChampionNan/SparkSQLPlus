create or replace view semiJoinView3510028673203223724 as select src as v2, dst as v4 from Graph AS g2 where (dst) in (select (src) from Graph AS g3);
create or replace view semiJoinView6808121210791406778 as select src as v1, dst as v2 from Graph AS g1 where (dst) in (select (v2) from semiJoinView3510028673203223724);
create or replace view semiEnum4372657119265073695 as select v1, v4, v2 from semiJoinView6808121210791406778 join semiJoinView3510028673203223724 using(v2);
create or replace view semiEnum8842658543025562334 as select v1, v4, v2, dst as v6 from semiEnum4372657119265073695, Graph as g3 where g3.src=semiEnum4372657119265073695.v4;
select v1, v2, v4, v6 from semiEnum8842658543025562334;
