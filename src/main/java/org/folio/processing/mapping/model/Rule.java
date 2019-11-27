package org.folio.processing.mapping.model;

public class Rule {
    private String fieldPath;
    private String ruleExpression;

    public Rule(String fieldPath, String ruleExpression) {
        this.fieldPath = fieldPath;
        this.ruleExpression = ruleExpression;
    }

    public String getFieldPath() {
        return fieldPath;
    }

    public void setFieldPath(String fieldPath) {
        this.fieldPath = fieldPath;
    }

    public String getRuleExpression() {
        return ruleExpression;
    }

    public void setRuleExpression(String ruleExpression) {
        this.ruleExpression = ruleExpression;
    }
}
