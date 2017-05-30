import XCTest
@testable import SwiftKueryMySQLTests

XCTMain([
     testCase(TestSelect.allTests),
     testCase(TestInsert.allTests),
     testCase(TestUpdate.allTests),
     testCase(TestAlias.allTests),
     testCase(TestParameters.allTests),
     testCase(TestJoin.allTests),
     testCase(TestSubquery.allTests),
     testCase(TestTransaction.allTests),
     testCase(TestColumnTypes.allTests),
     testCase(TestSchema.allTests)
])
