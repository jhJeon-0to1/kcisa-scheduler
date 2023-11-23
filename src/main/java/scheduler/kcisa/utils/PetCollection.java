package scheduler.kcisa.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Objects;

public class PetCollection {
    private static void downloadZipFile(String fileURL, String saveDir) throws IOException {
        URL url = new URL(fileURL);
        try (BufferedInputStream in = new BufferedInputStream(url.openStream());
             FileOutputStream fileOutputStream = new FileOutputStream(saveDir)) {
            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        }
    }


    private static void unzip(String zipFilePath, String outputDir, String fileName) throws IOException {
        try (ZipFile zipFile = new ZipFile(zipFilePath)) {
            Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();

            while (entries.hasMoreElements()) {
                ZipArchiveEntry entry = entries.nextElement();
                File newFile = new File(outputDir, fileName);

                if (entry.isDirectory()) {
                    newFile.mkdirs();
                } else {
                    new File(newFile.getParent()).mkdirs();
                    try (InputStream is = zipFile.getInputStream(entry);
                         OutputStream fos = new FileOutputStream(newFile)) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = is.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                }
            }
        }
    }

    private static void preprocessCSV(String inputPath, String outPath) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(inputPath)), "CP949"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(outPath)), StandardCharsets.UTF_8));
        String line = "";
        while ((line = br.readLine()) != null) {
            line = line.replaceAll("\"", "");
            bw.write(line + "\n");
        }
        br.close();
        bw.close();
    }

    private static JsonNode convertCsvToJsonNode(String csvFilePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();

        String[] headers = {
                "번호", "개방서비스명", "개방서비스아이디", "개방자치단체코드", "관리번호", "인허가일자",
                "인허가취소일자", "영업상태구분코드", "영업상태명", "상세영업상태코드", "상세영업상태명",
                "폐업일자", "휴업시작일자", "휴업종료일자", "재개업일자", "소재지전화", "소재지면적",
                "소재지우편번호", "소재지전체주소", "도로명전체주소", "도로명우편번호", "사업장명",
                "최종수정시점", "데이터갱신구분", "데이터갱신일자", "업태구분명", "좌표정보(x)",
                "좌표정보(y)", "업무구분명", "상세업무구분명", "권리주체일련번호", "총직원수"
        };

        CSVFormat format = CSVFormat.Builder.create().setRecordSeparator("\n").setHeader(headers).setSkipHeaderRecord(true)
                .setDelimiter(',')
                .setNullString("")
                .build();


        try (Reader reader = new FileReader(csvFilePath);
             CSVParser parser = new CSVParser(reader, format)) {
            for (CSVRecord record : parser) {
                String code = record.get("영업상태구분코드");
                if (code.equals("01")) {
                    String address = record.get("소재지전체주소");
                    if (address == null || address.isEmpty()) {
                        address = record.get("도로명전체주소");
                    }
                    address = address.split(" ")[0];
                    if (jsonNode.has(address)) {
                        jsonNode.put(address, jsonNode.get(address).asInt() + 1);
                    } else {
                        jsonNode.put(address, 1);
                    }
                }
            }
        }
        int total = 0;
        for (JsonNode node : jsonNode) {
            total += node.asInt();
        }
        jsonNode.put("전국", total);

        return jsonNode;
    }

    public static JsonNode getPetData(String fileURL, String saveDir, String zipFileName, String csvFileName) throws IOException {
        downloadZipFile(fileURL, saveDir + zipFileName);
        unzip(saveDir + zipFileName, saveDir, csvFileName);
        preprocessCSV(saveDir + csvFileName, saveDir + "preprocessed_" + csvFileName);

        JsonNode result = convertCsvToJsonNode(saveDir + "preprocessed_" + csvFileName);

        File file = new File(saveDir);
        File[] tempFile = file.listFiles();
        for (File value : Objects.requireNonNull(tempFile)) {
            if (value.getName().equals(csvFileName)) {
                value.delete();
            } else if (value.getName().equals("preprocessed_" + csvFileName)) {
                value.delete();
            }
        }

        return result;
    }
}
