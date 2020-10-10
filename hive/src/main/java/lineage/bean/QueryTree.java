package lineage.bean;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Sub query tree
 */
public class QueryTree {
    /**
     * sub query node id
     */
    private int id;
    /**
     * parent query tree id
     */
    private int pId;
    /**
     * current node name
     */
    private String current;
    /**
     * parent node name
     */
    private String parent;
    /**
     * table list
     */
    private Set<String> tableSet = new HashSet<String>();
    /**
     * child query list
     */
    private List<QueryTree> childList = new ArrayList<QueryTree>();
    /**
     * table column list
     */
    private List<ColumnLineage> colLineageList = new ArrayList<ColumnLineage>();

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getpId() {
        return pId;
    }

    public void setpId(int pId) {
        this.pId = pId;
    }

    public String getCurrent() {
        return current;
    }

    public void setCurrent(String current) {
        this.current = current;
    }

    public Set<String> getTableSet() {
        return tableSet;
    }

    public void setTableSet(Set<String> tableSet) {
        this.tableSet = tableSet;
    }

    public void addTableSet(String tableQualityName) {
        this.tableSet.add(tableQualityName);
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public List<QueryTree> getChildList() {
        return childList;
    }

    public void setChildList(List<QueryTree> childList) {
        this.childList = childList;
    }

    public List<ColumnLineage> getColLineageList() {
        return colLineageList;
    }

    public void setColLineageList(List<ColumnLineage> colLineageList) {
        this.colLineageList = colLineageList;
    }
}