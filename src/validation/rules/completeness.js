const completeness = {
  validate: (application) => {
    // 8. missing health questions causes COMPLETENESS_001 failure
    if (
      !application.healthQuestions ||
      application.healthQuestions.length === 0
    ) {
      return {
        status: "FAIL",
        messages: ["COMPLETENESS_001: Missing health questions."],
      };
    }
    return { status: "PASS", messages: [] };
  },
};

module.exports = completeness;
