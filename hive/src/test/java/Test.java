import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName Test
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/24 15:54
 * @Version 1.0
 **/
public class Test {
    public static void main(String[] args) {
        List<String> ret = new ArrayList<>();
        getFilesDir(ret);
        for (String str: ret){
            System.out.println(str);
        }
    }

    private static void getFilesDir(List<String> strs){
        strs.add("cz");
        strs.add("sen");

    }
}
