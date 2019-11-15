package nl.bitmanager.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class ActionDefinition {
    public final String id;
    public final String name;
    public final Logger logger;

    public final boolean debug;

    protected ActionDefinition (String name, boolean debug) {
        this.name = name;
        this.debug = debug;
        this.id = name; //Utils.getTrimmedClass(this);
        this.logger = LogManager.getLogger(name);
    }
}
