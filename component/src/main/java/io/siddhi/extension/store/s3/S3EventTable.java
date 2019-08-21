package io.siddhi.extension.store.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.table.record.AbstractRecordTable;
import io.siddhi.core.table.record.ExpressionBuilder;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.util.AnnotationHelper;
import jdk.nashorn.internal.IntDeque;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a sample class-level comment, explaining what the Sink extension class does.
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //AbstractRecordTable configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter",
 *                               description= "The description of the first parameter",
 *                               type =  "Supported parameter types.
 *                                        eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         In Source, only use static parameter)",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                          according to the type."),
 * {@literal @}Parameter(name = "The name of the second parameter",
 *                               description= "The description of the second parameter",
 *                               type =   "Supported parameter types.
 *                                         eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         In Source, only use static parameter)",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                         according to the type."),
 * },
 * //If AbstractRecordTable system configurations will need then
 * systemParameters = {
 * {@literal @}SystemParameter(name = "The name of the first  system parameter",
 *                                      description="The description of the first system parameter." ,
 *                                      defaultValue = "the default value of the system parameter.",
 *                                      possibleParameter="the possible value of the system parameter.",
 *                               ),
 * },
 * examples = {
 * {@literal @}Example(syntax = "sample query that explain how extension use in Siddhi."
 *                              description =" The description of the given example's query."
 *                      ),
 * }
 * )
 * </code></pre>
 */

@Extension(
        name = "s3",
        namespace = "store",
        description = "Lorem ipsum dolor sit amet",
        parameters = {
                @Parameter(
                        name = "credential.provider",
                        type = DataType.STRING,
                        description = "AWS credential provider to be used. If blank, default chain will be triggered."
                ),
                @Parameter(
                        name = "bucket.name",
                        type = DataType.STRING,
                        description = "Name of the S3 bucket"
                ),
                @Parameter(
                        name = "region",
                        type = DataType.STRING,
                        description = "Region of the deployment",
                        optional = true,
                        defaultValue = "standard"
                ),
                @Parameter(
                        name = "object.fields",
                        type = DataType.STRING,
                        description = "Comma separated ist of attributes which contains the object fields"
                ),
                @Parameter(
                        name = "content.type",
                        type = DataType.STRING,
                        description = "The content type of the object",
                        optional = true,
                        defaultValue = "application/octet-stream"
                )
        },
        examples = {
                @Example(
                        syntax = "Lorem ipsum dolor sit amet",
                        description = "Lorem ipsum dolor sit amet"
                )
        }
)

// for more information refer https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/query-guide-5.x/#table

public class S3EventTable extends AbstractRecordTable {

    private S3StoreConfig config;
    private AmazonS3 client;
    private List<Attribute> attributes;
    private int primaryKeyIndex;
    private List<Integer> objectIndices;

    /**
     * Initializing the Record Table
     *
     * @param tableDefinition definintion of the table with annotations if any
     * @param configReader    this hold the {@link AbstractRecordTable} configuration reader.
     */
    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        System.out.println(">>>>>>>>>> s3:init()");
        // Get attributes
        this.attributes = tableDefinition.getAttributeList();

        // Get annotations
        Annotation storeAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_STORE,
                tableDefinition.getAnnotations());
        Annotation primaryKeyAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                tableDefinition.getAnnotations());

        if (primaryKeyAnnotation == null) {
            throw new IllegalArgumentException("Primary key cannot be null")
        }

        if (primaryKeyAnnotation.getElements().size() > 1) {
            throw new IllegalArgumentException("Primary key can contain only one value.");
        }

        Element primaryKey = primaryKeyAnnotation.getElements().get(0);
        int primaryKeyIndex = -1;
        for (int i = 0; i < this.attributes.size(); i++) {
            if (this.attributes.get(i).getName().equalsIgnoreCase(primaryKey.getValue())) {
                primaryKeyIndex = i;
                break;
            }
        }

        if (attributes.get(primaryKeyIndex).getType() != Attribute.Type.STRING) {
            throw new IllegalArgumentException("Primary key can contain only STRING.");
        }

        // Initialize store configurations from the store annotation
        this.config = new S3StoreConfig(storeAnnotation);

        for (int i = 0; i < this.attributes.size(); i++) {
            if (this.config.getObjectFields().contains(this.attributes.get(i).getName().toLowerCase())) {
                this.objectIndices.add(i);
            }
        }
    }

    /**
     * Add records to the Table
     *
     * @param records records that need to be added to the table, each Object[] represent a record and it will match
     *                the attributes of the Table Definition.
     */
    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {
        records.forEach(record -> {
            System.out.println(">>>>>>>>>>> records inserted...");
            // Get the key field value
            Object key = record[primaryKeyIndex];
            if (key == null) {
                System.out.println("Cannot execute insert: null value detected for " + this.attributes.get(primaryKeyIndex).getName() + " field.");
                return;
            }

            // Build the payload
            HashMap<String, Object> payload = new HashMap<>();
            objectIndices.forEach(index -> {
                payload.put(this.attributes.get(index).getName(), record[index]);
            });

            // Convert the payload into an input stream
            InputStream inputStream = null;
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(payload);
                oos.flush();
                oos.close();
                inputStream = new ByteArrayInputStream(baos.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Populate object metadtaa
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType(config.getContentType());

            // todo set metadata fields

            PutObjectRequest req = new PutObjectRequest(config.getBucketName(), (String) key, inputStream, metadata);
            this.client.putObject(req);
        });
    }

    /**
     * Find records matching the compiled condition
     *
     * @param findConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                  compiled condition
     * @param compiledCondition         the compiledCondition against which records should be matched
     * @return RecordIterator of matching records
     */
    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        System.out.println(">>>>>>>>>> s3:find()");
        return null;
    }

    /**
     * Check if matching record exist or not
     *
     * @param containsConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                      compiled condition
     * @param compiledCondition             the compiledCondition against which records should be matched
     * @return if matching record found or not
     */
    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        System.out.println(">>>>>>>>>> s3:contains()");
        return false;
    }

    /**
     * Delete all matching records
     *
     * @param deleteConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition
     * @param compiledCondition            the compiledCondition against which records should be matched for deletion
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     **/
    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        System.out.println(">>>>>>>>>> s3:delete()");
    }

    /**
     * Update all matching records
     *
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated
     * @param map               the attributes and values that should be updated if the condition matches
     * @param list1             the attributes and values that should be updated for the matching records
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                          Map<String, CompiledExpression> map, List<Map<String, Object>> list1)
            throws ConnectionUnavailableException {
        System.out.println(">>>>>>>>>> s3:update()");
    }

    /**
     * Try updating the records if they exist else add the records
     *
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param map               the attributes and values that should be updated if the condition matches
     * @param list1             the values for adding new records if the update condition did not match
     * @param list2             the attributes and values that should be updated for the matching records
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                               Map<String, CompiledExpression> map, List<Map<String, Object>> list1,
                               List<Object[]> list2) throws ConnectionUnavailableException {
        System.out.println(">>>>>>>>>> s3:updateOrAdd()");
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        System.out.println(">>>>>>>>>> s3:compileCondition()");
        return null;
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        System.out.println(">>>>>>>>>> s3:compuleSetAttribute()");
        return null;
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     *
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void connect() throws ConnectionUnavailableException {
        System.out.println(">>>>>>>>>> s3:connect()");

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withRegion(config.getRegion());
//         if (this.config.getCredentialProvider() != null) {
//             // todo set the correct credential provider
//         }
        this.client = builder.build();
        createBucketIfNotExist(config.getBucketName(), config.getRegion());
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect.
     */
    @Override
    protected void disconnect() {
        System.out.println(">>>>>>>>>> s3:disconnect()");
    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    protected void destroy() {
        System.out.println(">>>>>>>>>> s3:destroy()");
    }

    private void createBucketIfNotExist(String bucketName, String region) {
        if (this.client.doesBucketExistV2(bucketName)) {
            System.out.println("Bucket " + bucketName + " already exist");
            return;
        }
        System.out.println("Bucket " + bucketName + " does not exist. Hence creating...");
        this.client.createBucket(new CreateBucketRequest(bucketName, region));
    }
}
