package db.etcd;

import com.alibaba.fastjson.JSONObject;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.options.PutOption;
import site.ycsb.*;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DBLayer extends DB {
    private Client client;
    private KV kv;


    private long timeout;
    private TimeUnit timeoutUnit;

    private Charset charset;

    private ByteSequence keyToByteSequence(String s) {
        return ByteSequence.from(s, this.charset);
    }

    private ByteSequence valuesToByteSequence(Map<String, ByteIterator> values) {
        JSONObject out = new JSONObject();
        values.forEach((k,v)->out.put(k, v.toString()));
        return ByteSequence.from(out.toString(), this.charset);
    }

    private Map<String, String> byteSequenceToValues(ByteSequence bs) {
        JSONObject in = JSONObject.parseObject(bs.toString(this.charset));
        Map<String, String> res = new HashMap<>();
        in.forEach((k,v)->in.put(k, v.toString()));
        return res;
    }

    @Override
    public void init() throws DBException {
        super.init();

        String endpoints = "";

        synchronized (DBLayer.class) {
            Properties prop = getProperties();
            endpoints = prop.getProperty("etcd.endpoints");
        }

        if (endpoints.isEmpty()) {
            throw new DBException();
        }

        this.client = Client.builder().endpoints("", "").build();
        this.kv = this.client.getKVClient();
    }


    @Override
    public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> results) {
        try {
            GetResponse resp = this.kv.get(keyToByteSequence(key)).get(timeout, timeoutUnit);
            if (resp.getCount() == 0) {
                return Status.NOT_FOUND;
            } else if (resp.getCount() > 1) {
                return Status.UNEXPECTED_STATE;
            } else {
                Map<String,String> fieldValues = byteSequenceToValues(resp.getKvs().get(0).getValue());

                if (results != null && fields != null) {
                    for (String field : fields) {
                        String value = fieldValues.get(field);
                        results.put(field, new StringByteIterator(value));
                    }
                }

                return Status.OK;
            }
        }  catch (InterruptedException | ExecutionException | TimeoutException e) {
            return Status.ERROR;
        }
    }

    @Override
    public Status scan(String tableName, String key, int limit, Set<String> fields, Vector<HashMap<String, ByteIterator>> results) {
        return Status.BAD_REQUEST;
    }

    @Override
    public Status update(String tableName, String key, Map<String, ByteIterator> values) {
        try {
            PutResponse resp = this.kv.put(keyToByteSequence(key), valuesToByteSequence(values), PutOption.newBuilder().withPrevKV().build()).get(timeout, timeoutUnit);
            return !resp.hasPrevKv() ? Status.UNEXPECTED_STATE : Status.OK;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return Status.ERROR;
        }
    }

    @Override
    public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
        try {
            this.kv.put(keyToByteSequence(key), valuesToByteSequence(values)).get(timeout, timeoutUnit);
            return Status.OK;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return Status.ERROR;
        }
    }

    @Override
    public Status delete(String tableName, String key) {
        try {
            DeleteResponse resp = this.kv.delete(keyToByteSequence(key)).get(timeout, timeoutUnit);
            return resp.getDeleted() == 0 ? Status.NOT_FOUND : Status.OK;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return Status.ERROR;
        }
    }

    @Override
    public void cleanup() throws DBException {
        this.client.close();
        super.cleanup();
    }
}
