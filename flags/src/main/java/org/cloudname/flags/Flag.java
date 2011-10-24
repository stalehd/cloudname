package org.cloudname.flags;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation used to define a field that should be configurable
 * via command line arguments.
 * 
 * @author acidmoose
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Flag {
    
    /**
     * The name used in command line argument. e.g. "bar" in "java Foo --bar 1".
     * @return
     */
    String name();
    
    /**
     * The default value of the field, if it is not given as a command line argument.
     * Overridden by required().
     * @return
     */
    String defaultValue();
    
    /**
     * A description of the field. Visible when running "--help".
     * @return
     */
    String description() default "";
    
    /**
     * Defines if a field is required to be present in the String[] or not.
     * Overrides defaultValue.
     * @return
     */
    boolean required() default false;
}