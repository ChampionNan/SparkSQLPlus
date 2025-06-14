package sqlplus.springboot.dto;

public class CustomDbmsResponse {
    private boolean connectionSuccess;
    private String message;

    // 默认构造函数
    public CustomDbmsResponse() {}

    // 带参数的构造函数
    public CustomDbmsResponse(boolean connectionSuccess, String message) {
        this.connectionSuccess = connectionSuccess;
        this.message = message;
    }

    // Getter 和 Setter
    public boolean isConnectionSuccess() {
        return connectionSuccess;
    }

    public void setConnectionSuccess(boolean connectionSuccess) {
        this.connectionSuccess = connectionSuccess;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}