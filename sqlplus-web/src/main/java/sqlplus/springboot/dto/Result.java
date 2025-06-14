package sqlplus.springboot.dto;

public class Result {
    public final static String SUCCESS = "success";
    public final static String FALLBACK = "fallback";

    private int code;
    private String message = SUCCESS;
    private String ddl_name = "";
    private Object data;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getDdl_name() { return this.ddl_name; }

    public void setDdl_name(String ddl_name) { this.ddl_name = ddl_name; }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
