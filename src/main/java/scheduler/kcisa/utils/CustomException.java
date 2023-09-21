package scheduler.kcisa.utils;

public class CustomException extends Exception {
    private final String code;

    public CustomException(String code, String message) {
        super(buildMessage(code, message));

        this.code = code;
    }

    private static String buildMessage(String code, String message) {
        StringBuilder sb = new StringBuilder();

        switch (code) {
            case "001":
                sb.append("수집 중 에러 발생 (").append(message).append(")");
                break;
            case "002":
                sb.append("데이터 저장 중 에러 발생 (").append(message).append(")");
                break;
            case "003":
                sb.append("데이터 계산 중 에러 발생 (").append(message).append(")");
                break;
        }

        return sb.toString();
    }

    public String getCode() {
        return code;
    }
}
