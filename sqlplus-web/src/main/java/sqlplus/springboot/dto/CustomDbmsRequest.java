package sqlplus.springboot.dto;

public class CustomDbmsRequest {
    private String dbmsName;
    private String url;
    private String username;
    private String password;

    // 构造函数
    public CustomDbmsRequest() {}

    public CustomDbmsRequest(String dbmsName, String url, String username, String password) {
        this.dbmsName = dbmsName;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    // Getter 和 Setter
    public String getDbmsName() {
        return dbmsName;
    }

    public void setDbmsName(String dbmsName) {
        this.dbmsName = dbmsName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}