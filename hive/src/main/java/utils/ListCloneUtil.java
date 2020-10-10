package utils;

import lineage.bean.ColumnLineage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @ClassName ListCloneUtil
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 17:20
 * @Version 1.0
 **/
public class ListCloneUtil {
    public static ColumnLineage cloneColLine(ColumnLineage col) {
        return new ColumnLineage(
                col.getToNameParse(),
                col.getColConditions(),
                cloneSet(col.getFromNameSet()),
                cloneSet(col.getConditionSet()),
                col.getToTable(),
                col.getToName());
    }


    public static Set<String> cloneSet(Set<String> set) {
        Set<String> set2 = new HashSet<String>(set.size());
        for (String string : set) {
            set2.add(string);
        }
        return set2;
    }

    public static List<ColumnLineage> cloneList(List<ColumnLineage> list) {
        List<ColumnLineage> list2 = new ArrayList<ColumnLineage>(list.size());
        for (ColumnLineage col : list) {
            list2.add(cloneColLine(col));
        }
        return list2;
    }
}
