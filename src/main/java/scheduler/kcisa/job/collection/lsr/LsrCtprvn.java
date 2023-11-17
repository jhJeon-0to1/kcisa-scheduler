package scheduler.kcisa.job.collection.lsr;

import java.util.Arrays;
import java.util.List;

public class LsrCtprvn {
    private final List<Ctprvn> lsrCtprvnList;

    public LsrCtprvn() {
        lsrCtprvnList = Arrays.asList(
                new Ctprvn("전체 지역", "", "00"),
                new Ctprvn("서울시", "11", "11"),
                new Ctprvn("인천시", "23", "28"),
                new Ctprvn("경기도", "31", "41"),
                new Ctprvn("부산시", "21", "26"),
                new Ctprvn("대구시", "22", "27"),
                new Ctprvn("광주시", "24", "29"),
                new Ctprvn("대전시", "25", "30"),
                new Ctprvn("울산시", "26", "31"),
                new Ctprvn("세종시", "29", "36"),
                new Ctprvn("강원도", "32", "51"),
                new Ctprvn("충청북도", "33", "43"),
                new Ctprvn("충청남도", "34", "44"),
                new Ctprvn("전라북도", "35", "45"),
                new Ctprvn("전라남도", "36", "46"),
                new Ctprvn("경상북도", "37", "47"),
                new Ctprvn("경상남도", "38", "48"),
                new Ctprvn("제주도", "39", "50")
        );
    }

    public List<Ctprvn> getLsrCtprvnList() {
        return lsrCtprvnList;
    }

    protected class Ctprvn {
        private String code;
        private String name;
        private String dbCode;

        public Ctprvn(String name, String code, String dbCode) {
            this.code = code;
            this.name = name;
            this.dbCode = dbCode;
        }

        public String getCode() {
            return code;
        }

        public String getName() {
            return name;
        }

        public String getDbCode() {
            return dbCode;
        }
    }
}


