package org.embulk.output.swift;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.embulk.config.*;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.util.RetryExecutor;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.slf4j.Logger;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public class SwiftFileOutputPlugin
        implements FileOutputPlugin {
    public interface PluginTask
            extends Task {
        @Config("username")
        public String getUsername();

        @Config("password")
        public String getPassword();

        @Config("auth_url")
        public String getAuthUrl();

        @Config("auth_type")
        public String getAuthType();

        @Config("tenant_id")
        @ConfigDefault("null")
        public Optional<String> getTenantId();

        @Config("tenant_name")
        @ConfigDefault("null")
        public Optional<String> getTenantName();

        @Config("container")
        public String getContainer();

        @Config("path_prefix")
        public String getPathPrefix();

        @Config("file_ext")
        public String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\"%03d.%02d\"")
        public String getSequenceFormat();

        @Config("max_connection_retry")
        @ConfigDefault("10") // 10 times retry to connect Azure Blob Storage if failed.
        public int getMaxConnectionRetry();
    }

    /**
     * Logger
     */
    private static final Logger LOGGER = Exec.getLogger(SwiftFileOutputPlugin.class);

    private  Account getAccount(PluginTask task) {
        AccountConfig accountConfig = new AccountConfig();

        String auth_type = task.getAuthType();
        accountConfig.setAuthUrl(task.getAuthUrl());
        accountConfig.setUsername(task.getUsername());
        accountConfig.setPassword(task.getPassword());

        Optional<String> tenant_id = task.getTenantId();
        if (tenant_id.isPresent()) {
            accountConfig.setTenantId(tenant_id.get());
        }
        Optional<String> tenant_name = task.getTenantName();
        if (tenant_name.isPresent()) {
            accountConfig.setTenantName(tenant_name.get());
        }

        if (auth_type.equals("keystone")) {
            if (!tenant_id.isPresent() && !tenant_name.isPresent()) {
                throw new ConfigException("if you choose keystone auth, you must specify to either tenant_id or tenant_name.");
            }
            accountConfig.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);
        } else if (auth_type.equals("tempauth")) {
            accountConfig.setAuthenticationMethod(AuthenticationMethod.TEMPAUTH);
        } else if (auth_type.equals("basic")) {
            accountConfig.setAuthenticationMethod(AuthenticationMethod.BASIC);
        } else {
            throw new ConfigException("auth_type has to be either keystone, tempauth or basic.");
        }

        return new AccountFactory(accountConfig).createAccount();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
                                  FileOutputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        {
            Account account = this.getAccount(task);
            Container targetContainer = account.getContainer(task.getContainer());
            if (targetContainer.exists() == false) {
                targetContainer.create();
            }
        }

        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileOutputPlugin.Control control) {
        control.run(taskSource);

        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<TaskReport> successTaskReports) {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        Account account = this.getAccount(task);
        return new SwiftFileOutput(account, task, taskIndex);
    }

    public static class SwiftFileOutput implements TransactionalFileOutput {
        private final Account account;
        private final String containerName;
        private final String pathPrefix;
        private final String sequenceFormat;
        private final String pathSuffix;
        private final int maxConnectionRetry;
        private BufferedOutputStream output = null;
        private int fileIndex;
        private File file;
        private String filePath;
        private int taskIndex;

        public SwiftFileOutput(Account account, PluginTask task, int taskIndex) {
            this.account = account;
            this.containerName = task.getContainer();
            this.taskIndex = taskIndex;
            this.pathPrefix = task.getPathPrefix();
            this.sequenceFormat = task.getSequenceFormat();
            this.pathSuffix = task.getFileNameExtension();
            this.maxConnectionRetry = task.getMaxConnectionRetry();
        }

        @Override
        public void nextFile() {
            closeFile();

            try {
                String suffix = pathSuffix;
                if (!suffix.startsWith(".")) {
                    suffix = "." + suffix;
                }
                filePath = pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + suffix;
                file = File.createTempFile(filePath, ".tmp");
                LOGGER.info("Writing local file {}", file.getAbsolutePath());
                output = new BufferedOutputStream(new FileOutputStream(file));
            } catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        private void closeFile() {
            if (output != null) {
                try {
                    output.close();
                    fileIndex++;
                } catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
            }
        }

        @Override
        public void add(Buffer buffer) {
            try {
                output.write(buffer.array(), buffer.offset(), buffer.limit());
            } catch (IOException ex) {
                throw Throwables.propagate(ex);
            } finally {
                buffer.release();
            }
        }

        @Override
        public void finish() {
            close();
            uploadFile();
        }

        private Void uploadFile() {
            if (filePath != null) {
                try {
                    return retryExecutor()
                            .withRetryLimit(maxConnectionRetry)
                            .withInitialRetryWait(500)
                            .withMaxRetryWait(30 * 1000)
                            .runInterruptible(new RetryExecutor.Retryable<Void>() {
                                @Override
                                public Void call() throws URISyntaxException, IOException, RetryExecutor.RetryGiveupException {
                                    Container container = account.getContainer(containerName);
                                    StoredObject object = container.getObject(filePath);
                                    LOGGER.info("Upload start {} to {}", file.getAbsolutePath(), filePath);
                                    object.uploadObject(new BufferedInputStream(new FileInputStream(file)));
                                    LOGGER.info("Upload completed {} to {}", file.getAbsolutePath(), filePath);
                                    if (!file.delete()) {
                                        throw new ConfigException("Couldn't delete local file " + file.getAbsolutePath());
                                    }
                                    LOGGER.info("Delete completed local file {}", file.getAbsolutePath());
                                    return null;
                                }

                                @Override
                                public boolean isRetryableException(Exception exception) {
                                    return true;
                                }

                                @Override
                                public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                        throws RetryExecutor.RetryGiveupException {
                                    if (exception instanceof FileNotFoundException || exception instanceof URISyntaxException || exception instanceof ConfigException) {
                                        throw new RetryExecutor.RetryGiveupException(exception);
                                    }
                                    String message = String.format("put request failed. Retrying %d/%d after %d seconds. Message: %s",
                                            retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                                    if (retryCount % 3 == 0) {
                                        LOGGER.warn(message, exception);
                                    } else {
                                        LOGGER.warn(message);
                                    }
                                }

                                @Override
                                public void onGiveup(Exception firstException, Exception lastException)
                                        throws RetryExecutor.RetryGiveupException {
                                }
                            });
                } catch (RetryExecutor.RetryGiveupException ex) {
                    throw Throwables.propagate(ex.getCause());
                } catch (InterruptedException ex) {
                    throw Throwables.propagate(ex);
                }
            }
            return null;
        }

        @Override
        public void close() {
            closeFile();
        }

        @Override
        public void abort() {
        }

        @Override
        public TaskReport commit() {
            return Exec.newTaskReport();
        }
    }
}