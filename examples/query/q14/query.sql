SELECT SUM(g4.dst * g1.src), g2.dst, SUM(g5.src), SUM(1), COUNT(*), COUNT(1)
FROM Graph AS g1, Graph AS g2, Graph as g3, Graph as g4, Graph as g5
WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst=g4.src AND g4.dst=g5.src AND g1.src >= g5.dst
GROUP BY g2.src, g2.dst, g3.dst