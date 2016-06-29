package org.embulk.output.swift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.output.swift.SwiftFileOutputPlugin.PluginTask;
import org.embulk.EmbulkTestRuntime;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test code for SwiftFileOutputPlugin
 * This class is incomplete yet.
 * @author yuuzi41
 */
public class TestSwiftFileOutputPlugin
{
    private String LOCAL_PATH_PREFIX = Resources.getResource("data_a.csv").getPath();
    private FileOutputRunner runner;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private  SwiftFileOutputPlugin plugin;

    @Before
    public void setUp(){
        this.runner = new FileOutputRunner(this.runtime.getInstance(SwiftFileOutputPlugin.class));
        this.plugin = new SwiftFileOutputPlugin();
    }

    @Test
    public void testCleanup() {
        PluginTask task = config().loadConfig(PluginTask.class);
        this.plugin.cleanup(task.dump(), 0, Lists.<TaskReport>newArrayList());
    }

    @Test(expected = ConfigException.class)
    public void testUnsupportedAuthType() {
        PluginTask task = config()
                .set("auth_type", "hoge")
                .loadConfig(PluginTask.class);
        this.plugin.open(task.dump(), 0);
    }

    @Test(expected = ConfigException.class)
    public void testKeystoneAuthWithoutTenant() {
        PluginTask task = config()
                .set("auth_type", "keystone")
                .loadConfig(PluginTask.class);
        this.plugin.open(task.dump(), 0);
    }

    public ConfigSource config()
    {
        // This config is optimized for Swift All-In-One Vagrant provided by SwiftStack.
        return Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "swift")
                .set("auth_type", "tempauth")
                .set("auth_url", "http://localhost:8080/auth/v1.0")
                .set("username", "test:tester")
                .set("password", "testing")
                .set("container","embulk_output")
                .set("path_prefix", "data")
                .set("sequence_format", "\"%03d.%02d\"")
                .set("last_path", "")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());
    }

    private ImmutableMap<String, Object> inputConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", LOCAL_PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig) {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }
    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        return builder.build();
    }

    private ImmutableMap<String, Object> formatterConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("header_line", "false");
        builder.put("timezone", "Asia/Tokyo");
        return builder.build();
    }
}
