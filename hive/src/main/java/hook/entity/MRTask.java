package hook.entity;

import java.util.Map;

/**
 * @ClassName MRTask
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 16:55
 * @Version 1.0
 **/
public class MRTask {
    private String id;
    private String name;
    private String jobId;
    private Map<String, Object> workInfo;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Map<String, Object> getWorkInfo() {
        return workInfo;
    }

    public void setWorkInfo(Map<String, Object> workInfo) {
        this.workInfo = workInfo;
    }
}
