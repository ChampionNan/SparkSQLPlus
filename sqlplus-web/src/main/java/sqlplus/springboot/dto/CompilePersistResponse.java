package sqlplus.springboot.dto;

public class CompilePersistResponse {
    long experimentTime1; // original
    long experimentTime2; // Yannakakis

    public long getExperimentTime1() {
        return experimentTime1;
    }

    public long getExperimentTime2() {
        return experimentTime2;
    }

    public void setExperimentTime1(long experimentTime1) {
        this.experimentTime1 = experimentTime1;
    }

    public void setExperimentTime2(long experimentTime2) {
        this.experimentTime2 = experimentTime2;
    }
}
