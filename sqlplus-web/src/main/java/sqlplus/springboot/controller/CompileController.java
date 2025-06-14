package sqlplus.springboot.controller;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.io.FileUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import scala.Option;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.mutable.StringBuilder;
import sqlplus.catalog.CatalogManager;
import sqlplus.codegen.CodeGenerator;
import sqlplus.codegen.SparkSQLExperimentCodeGenerator;
import sqlplus.codegen.SparkSQLPlusExampleCodeGenerator;
import sqlplus.codegen.SparkSQLPlusExperimentCodeGenerator;
import sqlplus.compile.CompileResult;
import sqlplus.compile.SqlPlusCompiler;
import sqlplus.convert.*;
import sqlplus.expression.Expression;
import sqlplus.expression.Variable;
import sqlplus.expression.VariableManager;
import sqlplus.graph.*;
import sqlplus.parser.SqlPlusParser;
import sqlplus.parser.ddl.SqlCreateTable;
import sqlplus.plan.SqlPlusPlanner;
import sqlplus.plan.table.SqlPlusTable;
import sqlplus.springboot.dto.HyperGraph;
import sqlplus.springboot.dto.*;
import sqlplus.springboot.util.CustomQueryManager;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import sqlplus.springboot.rest.controller.RestApiController;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import com.fasterxml.jackson.databind.ObjectMapper; // 无下划线提示
import com.fasterxml.jackson.core.type.TypeReference;


import java.util.ArrayList;
import java.util.List;

// LOGGER
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// SQL
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

@RestController
public class CompileController {
    private scala.collection.immutable.List<Variable> outputVariables = null;

    private scala.collection.immutable.List<Tuple2<Variable, Expression>> computations = null;

    private boolean isFull = false;

    private boolean isFreeConnex = false;

    private scala.collection.immutable.List<Variable> groupByVariables = null;

    private scala.collection.immutable.List<Tuple3<Variable, String, scala.collection.immutable.List<Expression>>> aggregations = null;

    private scala.Option<TopK> optTopK = null;

    private List<Tuple3<JoinTree, ComparisonHyperGraph, scala.collection.immutable.List<ExtraCondition>>> candidates = null;

    private CatalogManager catalogManager = null;

    private VariableManager variableManager = null;

    private scala.collection.immutable.List<SqlPlusTable> tables = null;

    private String sql = null;

    private String ddl = null;

    private CompileResult compileResult = null;

    private List<Integer> candidataIndex = new ArrayList<>();
    private List<String>  candidataString = new ArrayList<>();
    private List<Double> candidataCost = new ArrayList<>();
    private List<List<NodeStat>> nodeStatMap = new ArrayList<>();

    private List<String> sqlQueries = null;

    private Integer selectIndex = -1;

    private long experimentTime1 = -1;

    private long experimentTime2 = -1;

    private static final int MAX_PRINT_ROWS = 20;

    private static final Logger LOGGER = LoggerFactory.getLogger(CompileController.class);

    private static String  DUCKDB_URL = "jdbc:duckdb:/Users/cbn/Desktop/graph_db";
    private static String  MYSQL_URL = "jdbc:mysql://localhost:3306/test";
    private static String MYSQL_USER = "test";
    private static String MYSQL_PASSWORD = "";
    private static String  POSTGRESQL_URL = "jdbc:postgresql://localhost:5432/test";
    private static String POSTGRESQL_USER = "test";
    private static String POSTGRESQL_PASSWORD = "";

    private static final String  JDBC_URL = "";

    // @PostMapping("/compile/submit")
    public Result submit(@RequestBody CompileSubmitRequest request) {
        try {
            SqlNodeList nodeList = SqlPlusParser.parseDdl(request.getDdl());
            catalogManager = new CatalogManager();
            catalogManager.register(nodeList);
            storeSourceTables(nodeList);
            SqlNode sqlNode = SqlPlusParser.parseDml(request.getQuery());
            sql = request.getQuery();

            SqlPlusPlanner sqlPlusPlanner = new SqlPlusPlanner(catalogManager);
            RelNode logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode);

            variableManager = new VariableManager();
            LogicalPlanConverter converter = new LogicalPlanConverter(variableManager, catalogManager);

            Context context = converter.traverseLogicalPlan(logicalPlan);
            boolean isAcyclic = converter.dryRun(context).nonEmpty();
            ConvertResult convertResult = null;
            if (isAcyclic) {
                convertResult = converter.convertAcyclic(context);
            } else {
                convertResult = converter.convertCyclic(context);
            }

            outputVariables = convertResult.outputVariables();
            computations = convertResult.computations();
            isFull = convertResult.isFull();
            groupByVariables = convertResult.groupByVariables();
            aggregations = convertResult.aggregations();
            optTopK = convertResult.optTopK();

            if (!isFull) {
                // is the query is non-full, we add DISTINCT keyword to SparkSQL explicitly
                sql = sql.replaceFirst("[s|S][e|E][l|L][e|E][c|C][t|T]", "SELECT DISTINCT");
            }

            candidates = scala.collection.JavaConverters.seqAsJavaList(converter.candidatesWithLimit(convertResult.candidates(), 4));
            return mkSubmitResult(candidates);
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    @PostMapping("/compile/submit")
    public Result submit_py(@RequestBody CompileSubmitRequest request) {
        try {
            SqlNodeList nodeList = SqlPlusParser.parseDdl(request.getDdl());
            catalogManager = new CatalogManager();
            List<SqlPlusTable> tables = catalogManager.register(nodeList);
            storeSourceTables(nodeList);
            SqlNode sqlNode = SqlPlusParser.parseDml(request.getQuery());
            sql = request.getQuery();
            ddl = request.getDdl();

            SqlPlusPlanner sqlPlusPlanner = new SqlPlusPlanner(catalogManager);
            RelNode logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode);

            variableManager = new VariableManager();
            Optional<String> orderBy =  Optional.of("fanout");
            Optional<Boolean> desc = Optional.of(false);
            Optional<Integer> limit = Optional.of(8);
            Optional<Boolean> fixRootEnable = Optional.of(false);
            Optional<Boolean> pruneEnable = Optional.of(false);
            LogicalPlanConverter converter = new LogicalPlanConverter(variableManager, catalogManager);
            RunResult runResult = converter.runAndSelect(logicalPlan, orderBy.orElse(""), desc.orElse(false), limit.orElse(Integer.MAX_VALUE), fixRootEnable.orElse(false), pruneEnable.orElse(false));

            ParseQueryResponse response = new ParseQueryResponse();
            response.setDdl(request.getDdl());
            response.setQuery(request.getQuery());
            response.setTables(tables.stream()
                    .map(t -> new Table(t.getTableName(), Arrays.stream(t.getTableColumnNames()).collect(Collectors.toList())))
                    .collect(Collectors.toList()));

            List<sqlplus.springboot.rest.object.JoinTree> joinTrees = JavaConverters.seqAsJavaList(runResult.candidates()).stream()
                    .map(t -> new RestApiController().buildJoinTree(t._1(), t._2(), t._3(), null))
                    .collect(Collectors.toList());
            response.setJoinTrees(joinTrees);

            List<Computation> computations = JavaConverters.seqAsJavaList(runResult.computations()).stream()
                    .map(c -> new Computation(c._1.name(), c._2.format()))
                    .collect(Collectors.toList());
            response.setComputations(computations);

            response.setOutputVariables(JavaConverters.seqAsJavaList(runResult.outputVariables()).stream().map(Variable::name).collect(Collectors.toList()));
            response.setGroupByVariables(JavaConverters.seqAsJavaList(runResult.groupByVariables()).stream().map(Variable::name).collect(Collectors.toList()));
            List<Aggregation> aggregations = JavaConverters.seqAsJavaList(runResult.aggregations()).stream()
                    .map(t -> new Aggregation(t._1().name(), t._2(), JavaConverters.seqAsJavaList(t._3()).stream().map(Expression::format).collect(Collectors.toList())))
                    .collect(Collectors.toList());
            response.setAggregations(aggregations);

            if (runResult.optTopK().nonEmpty()) {
                sqlplus.springboot.rest.object.TopK topK = new sqlplus.springboot.rest.object.TopK();
                topK.setOrderByVariable(runResult.optTopK().get().sortBy().name());
                topK.setDesc(runResult.optTopK().get().isDesc());
                topK.setLimit(runResult.optTopK().get().limit());
                response.setTopK(topK);
            }

            response.setFull(runResult.isFull());

            if (!runResult.isFull()) {
                // is the query is non-full, we add DISTINCT keyword to SparkSQL explicitly
                sql = sql.replaceFirst("[s|S][e|E][l|L][e|E][c|C][t|T]", "SELECT DISTINCT");
            }

            if (!sql.endsWith(";")) {
                sql = sql + ";";
            }
            response.setFreeConnex(runResult.isFreeConnex());

            Result result = new Result();
            result.setCode(200);
            result.setData(response);

            String ddl_name = "";
            String ddl = request.getDdl();
            if (ddl.contains("Graph") || ddl.contains("graph")) {
                ddl_name = "graph";
            } else if (ddl.contains("University") || ddl.contains("lsqb")) {
                ddl_name = "lsqb";
            } else if (ddl.contains("lineitem") || ddl.contains("tpch")) {
                ddl_name = "tpch";
            } else if (ddl.contains("aka_name") || ddl.contains("job")) {
                ddl_name = "job";
            } else {
                ddl_name = "custom";
            }

            result.setDdl_name(ddl_name);

            RestTemplate restTemplate = new RestTemplate();

            String pythonUrl = "http://localhost:8000/python-api";

            ResponseEntity<String> pythonResp = restTemplate.postForEntity(pythonUrl, result, String.class);


            if (pythonResp.getStatusCode() == HttpStatus.OK) {
                String responseJson = pythonResp.getBody();
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, Object> resultMap = mapper.readValue(responseJson, new TypeReference<Map<String, Object>>(){});

                    List<Map<String, Object>> dataList = (List<Map<String, Object>>) resultMap.get("data");
                    this.candidataString.clear();
                    this.candidataIndex.clear();
                    this.candidataCost.clear();
                    this.nodeStatMap.clear();
                    this.nodeStatMap = new ArrayList<>();
                    if (dataList != null) {
                        // 遍历数组里的每个 Map
                        for (Map<String, Object> item : dataList) {
                            // 建议用 Number 再转型，避免类型不一致引发异常
                            int index = ((Number) item.get("index")).intValue();
                            String queries = (String) item.get("queries");
                            double cost = ((Number) item.get("cost")).doubleValue();
                            List<List<Object>> nodeStatList = (List<List<Object>>) item.get("node_stat");
                            List<NodeStat> oneSetNodes = new ArrayList<>();
                            if (nodeStatList != null) {
                                for (List<Object> nodeData : nodeStatList) {
                                    int id = ((Number) nodeData.get(0)).intValue();
                                    String alias = (String) nodeData.get(1);
                                    double trueSize = ((Number) nodeData.get(2)).doubleValue();
                                    double estimateSize = ((Number) nodeData.get(3)).doubleValue();
                                    NodeStat nodeStat = new NodeStat(id, alias, trueSize, estimateSize);
                                    oneSetNodes.add(nodeStat);
                                    // parsedNodeStat.add(nodeStat);
                                }
                            }

                            this.candidataIndex.add(index);
                            this.candidataString.add(queries);
                            this.candidataCost.add(cost);
                            this.nodeStatMap.add(oneSetNodes);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("请求失败，状态码：" + pythonResp.getStatusCodeValue());
            }

            // TODO: add index setting
            scala.collection.immutable.List<scala.Tuple3<JoinTree, ComparisonHyperGraph, scala.collection.immutable.List<ExtraCondition>>> scalaCandidates = runResult.candidates();

            java.util.List<scala.Tuple3<JoinTree, ComparisonHyperGraph, scala.collection.immutable.List<ExtraCondition>>> javaCandidatesList =
                    scala.collection.JavaConverters.seqAsJavaList(scalaCandidates);

            java.util.List<scala.Tuple3<JoinTree, ComparisonHyperGraph, scala.collection.immutable.List<ExtraCondition>>> selectedCandidatesJava =
                    candidataIndex.stream()
                            .filter(index -> index >= 0 && index < javaCandidatesList.size()) // 过滤无效索引
                            .map(javaCandidatesList::get) // 根据索引获取候选项
                            .collect(Collectors.toList());

            scala.collection.immutable.List<scala.Tuple3<JoinTree, ComparisonHyperGraph, scala.collection.immutable.List<ExtraCondition>>> scalaSelectedCandidates =
                    scala.collection.JavaConverters.asScalaBuffer(selectedCandidatesJava).toList();

            candidates = scala.collection.JavaConverters.seqAsJavaList(converter.candidatesWithLimit(scalaSelectedCandidates, 4));
            return mkSubmitResult(candidates, ddl_name);
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    private void storeSourceTables(SqlNodeList nodeList) {
        ArrayList<SqlPlusTable> sourceTables = new ArrayList<>();
        //TODO: redundant code from catalogManager
        for (SqlNode node : nodeList) {
            SqlCreateTable createTable = (SqlCreateTable) node;
            SqlPlusTable table = new SqlPlusTable(createTable);
            sourceTables.add(table);
        }
        tables = scala.collection.JavaConverters.asScalaBuffer(sourceTables).toList();
    }

    private Result mkSubmitResult(List<Tuple3<JoinTree, ComparisonHyperGraph, scala.collection.immutable.List<ExtraCondition>>> candidates) {
        Result result = new Result();
        result.setCode(200);
        CompileSubmitResponse response = new CompileSubmitResponse();
        response.setCost(this.candidataCost);
        response.setNodeStatMap(this.nodeStatMap);
        response.setCandidates(candidates.stream().map(tuple -> {
            Candidate candidate = new Candidate();
            List<Relation> relations = extractRelations(tuple._1());
            List<BagRelation> bagRelations = relations.stream().filter(r -> r instanceof BagRelation).map(r -> (BagRelation)r).collect(Collectors.toList());
            // assign new name to bag relations
            Map<String, String> bagRelationNames = new HashMap<>();
            int suffix = 1;
            for (int i = 0; i < bagRelations.size(); i++) {
                bagRelationNames.put(bagRelations.get(i).getTableDisplayName(), "B" + suffix);
                suffix += 1;
            }

            Tree tree = Tree.fromJoinTree(tuple._1(), bagRelationNames);
            Set<JoinTreeEdge> joinTreeEdges = scala.collection.JavaConverters.setAsJavaSet(tuple._1().getEdges());
            List<String> joinTreeEdgeStrings = new ArrayList<>();
            Map<JoinTreeEdge, String> joinTreeEdgeToStringMap = new HashMap<>();

            joinTreeEdges.forEach(e -> {
                String s = mkUniformStringForJoinTreeEdge(e, bagRelationNames);
                joinTreeEdgeStrings.add(s);
                joinTreeEdgeToStringMap.put(e, s);
            });
            List<String> sortedEdgeStrings = joinTreeEdgeStrings.stream().sorted().collect(Collectors.toList());
            HyperGraph hyperGraph = HyperGraph.fromComparisonHyperGraphAndRelations(tuple._2(), sortedEdgeStrings, joinTreeEdgeToStringMap);

            Map<String, Object> bags = new HashMap<>();
            bagRelations.forEach(b -> visitBagRelation(b, bagRelationNames, bags));

            candidate.setTree(tree);
            candidate.setHyperGraph(hyperGraph);
            candidate.setBags(bags);
            return candidate;
        }).collect(Collectors.toList()));
        result.setDdl_name(ddl_name);
        result.setData(response);
        return result;
    }

    private String mkUniformStringForJoinTreeEdge(JoinTreeEdge edge, Map<String, String> bagRelationNames) {
        String name1 = "";
        String name2 = "";

        if (edge.getSrc() instanceof BagRelation) {
            name1 = bagRelationNames.get(edge.getSrc().getTableDisplayName());
        } else {
            name1 = edge.getSrc().getTableDisplayName();
        }

        if (edge.getDst() instanceof BagRelation) {
            name2 = bagRelationNames.get(edge.getDst().getTableDisplayName());
        } else {
            name2 = edge.getDst().getTableDisplayName();
        }

        if (name1.compareToIgnoreCase(name2) > 0) {
            return name1 + "-" + name2;
        } else {
            return name2 + "-" + name1;
        }
    }

    private void visitBagRelation(BagRelation bagRelation, Map<String, String> bagRelationNames, Map<String, Object> bags) {
        String name = bagRelation.getTableDisplayName();
        Map<String, Object> bag = new HashMap<>();
        List<Map<String, Object>> internals = new ArrayList<>();
        List<Relation> internalRelations = scala.collection.JavaConverters.seqAsJavaList(bagRelation.getInternalRelations());
        internalRelations.forEach(r -> {
            Map<String, Object> map = new HashMap<>();
            map.put("relation", r.getTableDisplayName());
            map.put("variables", new ArrayList<>(scala.collection.JavaConverters.seqAsJavaList(r.getVariableList()).stream().map(Variable::name).collect(Collectors.toList())));
            internals.add(map);
        });

        List<List<String>> connections = new ArrayList<>();
        for (int i = 0; i < internals.size() - 1; i++) {
            for (int j = i + 1; j < internals.size(); j++) {
                int finalJ = j;
                if (((List<String>)(internals.get(i).get("variables"))).stream().anyMatch(v -> ((List<String>)(internals.get(finalJ).get("variables"))).contains(v))) {
                    connections.add(Arrays.asList((String)internals.get(i).get("relation"), (String)internals.get(j).get("relation")));
                }
            }
        }

        bag.put("internals", internals);
        bag.put("connections", connections);
        bags.put(bagRelationNames.get(name), bag);
    }

    private List<Relation> extractRelations(JoinTree joinTree) {
        Set<Relation> set = new HashSet<>();
        scala.collection.JavaConverters.setAsJavaSet(joinTree.getEdges()).forEach(e -> {
            set.add(e.getSrc());
            set.add(e.getDst());
        });
        List<Relation> list = new ArrayList<>(set);
        list.sort(Comparator.comparingInt(Relation::getRelationId));
        return list;
    }

    // @PostMapping("/compile/candidate")
    public Result candidate(@RequestBody CompileCandidateRequest request) {
        Result result = new Result();
        result.setCode(200);

        ConvertResult convertResult = ConvertResult.buildFromSingleResult(
                candidates.get(request.getIndex()),
                outputVariables,
                computations,
                isFull,
                groupByVariables,
                aggregations,
                optTopK
        );

        SqlPlusCompiler sqlPlusCompiler = new SqlPlusCompiler(variableManager);
        compileResult = sqlPlusCompiler.compile(catalogManager, convertResult, false);
        CodeGenerator codeGenerator = new SparkSQLPlusExampleCodeGenerator(compileResult,
                "sqlplus.example", "SparkSQLPlusExample");
        StringBuilder builder = new StringBuilder();
        codeGenerator.generate(builder);

        CompileCandidateResponse response = new CompileCandidateResponse();
        response.setCode(builder.toString());
        result.setData(response);
        return result;
    }

    @PostMapping("/compile/candidate")
    public Result candidate_py(@RequestBody CompileCandidateRequest request) {
        Result result = new Result();
        result.setCode(200);
        CompileCandidateResponse response = new CompileCandidateResponse();
        selectIndex = request.getIndex();
        response.setCode(candidataString.get(selectIndex));
        result.setData(response);
        return result;
    }

    // @PostMapping("/compile/persist")
    public Result persist(@RequestBody CompilePersistRequest request) {
        Result result = new Result();
        result.setCode(200);

        String shortQueryName = CustomQueryManager.assign("examples/query/custom/");
        String queryName = shortQueryName.replace("q", "CustomQuery");
        String sqlplusClassname = queryName + "SparkSQLPlus";
        String sparkSqlClassname = queryName + "SparkSQL";

        String queryPath = "examples/query/custom/" + shortQueryName;
        File queryDirectory = new File(queryPath);
        queryDirectory.mkdirs();

        // generate sqlplus code
        CodeGenerator sqlplusCodeGen = new SparkSQLPlusExperimentCodeGenerator(compileResult, sqlplusClassname, queryName, shortQueryName);
        StringBuilder builder = new StringBuilder();
        sqlplusCodeGen.generate(builder);
        String sqlplusCodePath = queryPath + File.separator + sqlplusClassname + ".scala";
        try {
            FileUtils.writeStringToFile(new File(sqlplusCodePath), builder.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // generate SparkSQL code
        CodeGenerator sparkSqlCodeGen = new SparkSQLExperimentCodeGenerator(tables, sql, sparkSqlClassname, queryName, shortQueryName);
        builder.clear();
        sparkSqlCodeGen.generate(builder);
        String sparkSqlCodePath = queryPath + File.separator + sparkSqlClassname + ".scala";
        try {
            FileUtils.writeStringToFile(new File(sparkSqlCodePath), builder.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        CompilePersistResponse response = new CompilePersistResponse();
        // response.setName(queryName);
        // response.setPath(queryPath);
        result.setData(response);
        return result;
    }

    private static List<String> splitSqlQueries(String multiQuery) {
        // 使用分号分割，并去除空语句
        List<String> queries = new ArrayList<>();
        String[] splitQueries = multiQuery.split(";");
        for (String query : splitQueries) {
            String trimmedQuery = query.trim();
            if (!trimmedQuery.isEmpty()) {
                queries.add(trimmedQuery);
            }
        }
        return queries;
    }

    public long execQuery(String query, Connection connection, Statement statement) throws SQLException {
        // 跳过空语句
        if (query.trim().isEmpty()) {
            return 0;
        }

        long exetTime = -1;

        LOGGER.info("执行查询: {}", query);
        long startTime = System.nanoTime(); // 记录开始时间

        try (ResultSet resultSet = statement.executeQuery(query)) {
            long endTime = System.nanoTime(); // 记录结束时间
            long durationInMillis = (endTime - startTime) / 1_000_000; // 计算运行时间（毫秒）
            exetTime = durationInMillis;

            LOGGER.info("查询运行时间: {} 毫秒", durationInMillis);
            LOGGER.info("查询结果:");

            // 获取结果集的元数据以动态处理列
            int columnCount = resultSet.getMetaData().getColumnCount();

            // 打印列名
            StringBuilder header = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                header.append(resultSet.getMetaData().getColumnName(i)).append("\t");
            }
            LOGGER.info(header.toString());

            // 打印每一行
            int rowCount = 0;
            while (resultSet.next()) {
                if (rowCount >= MAX_PRINT_ROWS) {
                    LOGGER.info("结果被截断，显示前 {} 行。", MAX_PRINT_ROWS);
                    break;
                }
                rowCount ++;
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    row.append(resultSet.getString(i)).append("\t");
                }
                LOGGER.info(row.toString());
            }
        } catch (SQLException e) {
            LOGGER.error("执行查询时发生错误: {}", query, e);
        }

        LOGGER.info("--------------------------------------------------");
        return exetTime;
    }

    static class DatabaseConfig {
        private final String url;
        private final String user;
        private final String password;

        public DatabaseConfig(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        public String getUrl() {
            return url;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }
    }

    private static Connection getConnection(DatabaseConfig config) throws SQLException {
        if (config.getUser() != null && config.getPassword() != null) {
            return DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
        } else if (config.getUser() != null ) {
            return DriverManager.getConnection(config.getUrl(), config.getUser(), null);
        } else {
            return DriverManager.getConnection(config.getUrl());
        }
    }

    @PostMapping("/compile/persist")
    public Result persist_py(@RequestBody CompilePersistRequest request) {
        Result result = new Result();
        result.setCode(200);

        experimentTime1 = -1;
        experimentTime2 = -1;

        String db_type = request.getName();

        sqlQueries = splitSqlQueries(candidataString.get(selectIndex));
        DatabaseConfig db_config = null;
        switch (db_type) {
            case "duckdb":
                db_config = new DatabaseConfig(DUCKDB_URL, null, null);
                break;
            case "mysql":
                db_config = new DatabaseConfig(MYSQL_URL, MYSQL_USER, null);
                break;
            case "postgresql":
                db_config = new DatabaseConfig(POSTGRESQL_URL, POSTGRESQL_USER, null);
                break;
            default:
                LOGGER.error("没有登记该数据库的JDBC连接");
                break;
        }

        try (Connection connection = getConnection(db_config);
             Statement statement = connection.createStatement();) {

            LOGGER.info("成功连接到DuckDB数据库。");

            for (int i = 0; i < sqlQueries.size(); i++) {
                String query = sqlQueries.get(i);
                if (db_type.equals("mysql") && query.contains("TEMP")) {
                    // 替换"TEMP"为空字符串
                    query = query.replace(" TEMP", "");
                }
                if (i < sqlQueries.size() - 1) {
                    statement.executeUpdate(query);
                } else {
                    experimentTime2 = execQuery(query, connection, statement);
                }
            }
            experimentTime1 = execQuery(sql, connection, statement);

        } catch (SQLException e) {
            LOGGER.error("无法连接到数据库。", e);
        }

        CompilePersistResponse response = new CompilePersistResponse();
        response.setExperimentTime1(experimentTime1);
        response.setExperimentTime2(experimentTime2);
        result.setData(response);
        return result;
    }

    @PostMapping("/compile/custom-dbms")
    public Result customDbms(@RequestBody CustomDbmsRequest request) {
        Result result = new Result();

        try {
            // 验证请求参数
            if (request.getDbmsName() == null || request.getDbmsName().trim().isEmpty()) {
                CustomDbmsResponse response = new CustomDbmsResponse(false, "DBMS名称不能为空");
                result.setCode(400);
                result.setData(response);
                result.setMessage("参数错误");
                return result;
            }

            if (request.getUrl() == null || request.getUrl().trim().isEmpty()) {
                CustomDbmsResponse response = new CustomDbmsResponse(false, "数据库URL不能为空");
                result.setCode(400);
                result.setData(response);
                result.setMessage("参数错误");
                return result;
            }

            // 根据数据库类型创建配置
            String dbmsName = request.getDbmsName().toLowerCase().trim();

            DatabaseConfig db_config = null;

            // 构建完整的JDBC URL
            String jdbcPrefix = getDriverClass(dbmsName);
            if (jdbcPrefix == null) {
                LOGGER.error("没有登记该数据库的JDBC连接: {}", dbmsName);
                return null;
            }

            String fullUrl = jdbcPrefix + request.getUrl();

            System.out.println("请求失败，状态码：" + fullUrl);

            switch (dbmsName) {
                case "duckdb":
                    db_config = new DatabaseConfig(fullUrl, null, null);
                    break;
                case "mysql":
                case "postgresql":
                case "postgres":
                    db_config = new DatabaseConfig(fullUrl, request.getUsername(), request.getPassword());
                    break;
                default:
                    LOGGER.error("没有登记该数据库的JDBC连接: {}", dbmsName);
                    break;
            }

            if (db_config == null) {
                CustomDbmsResponse response = new CustomDbmsResponse(false, "不支持的数据库类型: " + request.getDbmsName());
                result.setCode(400);
                result.setData(response);
                result.setMessage("数据库类型错误");
                return result;
            }

            try (Connection connection = getConnection(db_config);
                 Statement statement = connection.createStatement();) {

                LOGGER.info("Set successfully! ");
                LOGGER.info(dbmsName);

                switch (dbmsName) {
                    case "duckdb":
                        DUCKDB_URL = db_config.getUrl();
                        break;
                    case "mysql":
                        MYSQL_URL = db_config.getUrl();
                        MYSQL_USER = db_config.getUser();
                        MYSQL_PASSWORD = db_config.getPassword();
                        break;
                    case "postgresql":
                    case "postgres":
                        POSTGRESQL_URL = db_config.getUrl();
                        POSTGRESQL_USER = db_config.getUser();
                        POSTGRESQL_PASSWORD = db_config.getPassword();
                        break;
                    default:
                        LOGGER.error("没有登记该数据库的JDBC连接: {}", dbmsName);
                        break;
                }
                CustomDbmsResponse response;
                response = new CustomDbmsResponse(true, "Successfully connect to " + request.getDbmsName().trim() + " database! ");
                result.setCode(200);
                result.setMessage("Success");
                result.setData(response);
            }
        } catch (Exception e) {
            CustomDbmsResponse response = new CustomDbmsResponse(false, "服务器内部错误: " + e.getMessage());
            result.setCode(500);
            result.setData(response);
            result.setMessage("服务器错误");
            LOGGER.error("数据库连接测试异常", e);
        }
        return result;
    }

    private String getDriverClass(String dbmsName) {
        switch (dbmsName.toLowerCase().trim()) {
            case "duckdb":
                return "jdbc:duckdb:";
            case "postgresql":
            case "postgres":
                return "jdbc:postgresql:";
            case "mysql":
                return "jdbc:mysql:";
            default:
                return null;
        }
    }
}
