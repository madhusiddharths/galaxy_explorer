import { useControls, button } from 'leva'
import { useEffect } from 'react'

const Controls = ({ onUpdate }) => {
    // Leva controls
    const [values, set] = useControls(() => ({
        "Year": { value: 2016, min: -100000, max: 100000, step: 100 },
        "Min Distance (ly)": { value: 10, min: 10, max: 17000, step: 10 },
        "Max Distance (ly)": { value: 400, min: 10, max: 17000, step: 10 },
        "Healpix Sector": { options: ["All", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], value: "All" },
        "Run Query": button((get) => {
            const hp = get("Healpix Sector") === "All" ? null : parseInt(get("Healpix Sector"))
            onUpdate({
                min_dist: get("Min Distance (ly)"),
                max_dist: get("Max Distance (ly)"),
                healpix: hp,
                year: get("Year")
            })
        })
    }))

    // Initial fetch on mount
    useEffect(() => {
        onUpdate({
            min_dist: 10,
            max_dist: 400,
            healpix: null,
            year: 2016
        })
    }, [])

    return null
}

export default Controls
