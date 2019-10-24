package bokarev;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class TextPair implements WritableComparable<TextPair>{
    public  Text code;
    public  Text flag;

    public  TextPair(){
        this.code=new Text();
        this.flag =new Text();
    }

    public TextPair(Text code, Text second) {
        this.code = code;
        this.flag = second;
    }

    public TextPair(String code, String second){
        this.code =new Text(code);
        this.flag =new Text(second);
    }

    public Text getCode() {
        return code;
    }

    public Text getFlag() {
        return flag;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextPair textPair = (TextPair) o;
        return Objects.equals(code, textPair.code) &&
                Objects.equals(flag, textPair.flag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, flag);
    }

    @Override
    public String toString() {
        return code +"\t"+ flag;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        code.readFields(in);
        flag.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        code.write(out);
        flag.write(out);
    }


    @Override
    public int compareTo(TextPair tp) {
        int cmp= code.compareTo(tp.getCode());
        if(cmp!=0)
            return cmp;
        return flag.compareTo(tp.getFlag());

    }
}