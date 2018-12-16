const parse = require('csv-parse')
const fs = require('fs')

let records = []

try {
    const stream = fs.createReadStream(__dirname + '/BlackFriday.csv')
    const streamParser = parse({ delimiter: ',', columns: true, skip_lines_with_error: true })

    streamParser.on('readable', () => {
        console.time('time')
        let row
        let user = {}
        while (row = streamParser.read()) {
            records.push(handlerUserProperties(row, user))
        }
    })
    streamParser.on('error', error => {
        if (error) throw new Error(error)
    })
    streamParser.on('end', () => {
        console.log(`
        Readed ${records.length} rows.
        `)
        console.timeEnd('time')
    })

    console.log('\nstart parsing..')
    stream.pipe(streamParser)

} catch (error) {
    console.log(error)
}

function handlerUserProperties(row, user) {
    if (!row || !user) throw new Error('parameters are not valid')

    if (user.userId === row.User_ID) {
        user[products] = {
            id: row.Product_ID,
            productCategory: row.Product_Category1,
            purchase: row.Purchase
        }
    } else {
        user = {
            userId: row.User_ID,
            gender: row.Gender,
            age: row.Age,
            products: {
                id: row.Product_ID,
                productCategory: row.Product_Category1,
                purchase: row.Purchase
            }
        }
    }
    return user
}
