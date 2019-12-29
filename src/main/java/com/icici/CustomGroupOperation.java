/**
 * 
 */
package com.icici;

import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;

import com.mongodb.DBObject;

/**
 * @author ntipl
 *
 */
public class CustomGroupOperation implements AggregationOperation {
    private DBObject operation;

    public CustomGroupOperation (DBObject operation) {
        this.operation = operation;
    }

    @Override
    public DBObject toDBObject(AggregationOperationContext context) {
        return context.getMappedObject(operation);
    }
}