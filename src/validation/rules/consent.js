const consent = {
  validate: (application) => {
    // 9. missing consent causes CONSENT_001 failure
    if (!application.consentGiven) {
      return { status: "FAIL", messages: ["CONSENT_001: Consent not given."] };
    }
    return { status: "PASS", messages: [] };
  },
};

module.exports = consent;
