package org.datacenter.kafka.sink.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class ConfigurationUtils {

    public static void copy(Configuration from, Configuration to) {
        for (Map.Entry<String, String> entry : from) {
            to.set(entry.getKey(), entry.getValue());
        }
    }

    public static Configuration readConfiguration(String[] resourcePaths) {

        Configuration result = new Configuration();

        for (String resourcePath : resourcePaths) {
            File resourcePathFile = new File(resourcePath);
            checkArgument(resourcePathFile.exists(), "File does not exist: %s", resourcePathFile);

            Configuration resourceProperties = new Configuration(false);
            // We need to call `getPath` instead of `toURI` because Hadoop library can not handle a
            // configuration resourcePath with relative Xinclude files in case of passing URI.
            // In details, see https://issues.apache.org/jira/browse/HADOOP-17088.
            resourceProperties.addResource(new Path(resourcePathFile.getPath()));
            copy(resourceProperties, result);
        }

        return result;
    }
}
